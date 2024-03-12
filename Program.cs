using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.IO.Compression;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;

namespace UploadProductionBackupOnGoogleDrive
{
    class Program
    {
        public static string ConnectionStrings = ConfigurationManager.AppSettings.Get("ConnectionStrings");
        static void Main(string[] args)
        {
            Guid sessionid = Guid.NewGuid();
            UpdateProcessStatus(38, sessionid, DateTime.Now, null, "START");
            Console.WriteLine("Started");
            string credentialsFilePath = ConfigurationManager.AppSettings["credentialsFilePath"];
            string folderId = ConfigurationManager.AppSettings["folderId"];
            string folderPath = ConfigurationManager.AppSettings["folderPath"];
            UploadBackupFilesToGoogleDrive(credentialsFilePath, folderId, folderPath);
            UpdateProcessStatus(38, sessionid, DateTime.Now, null, "Finished");
            Console.WriteLine("End");

        }

        public static void UploadBackupFilesToGoogleDrive(string credentialsFilePath, string folderId, string folderPath)
        {
            try
            {
                // Load credentials fro  m the JSON key file
                GoogleCredential credential;
                using (var stream = new FileStream(credentialsFilePath, FileMode.Open))
                {
                    credential = GoogleCredential.FromStream(stream)
                        .CreateScoped(new[] { DriveService.ScopeConstants.DriveFile });
                }

                // Create the Drive API service
                var service = new DriveService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = "ProductionBackup Upload On Google Drive"
                });

                // Get all BAK files in the folder
                string[] bakFiles = Directory.GetFiles(folderPath, "*.bak");

                foreach (string bakFile in bakFiles)
                {
                    // Convert BAK file to ZIP format
                    string zipFilePath = Path.Combine(Path.GetDirectoryName(bakFile), Path.GetFileNameWithoutExtension(bakFile) + ".zip");
                    using (var zipArchive = ZipFile.Open(zipFilePath, ZipArchiveMode.Create))
                    {
                        zipArchive.CreateEntryFromFile(bakFile, Path.GetFileName(bakFile));
                    }

                    // Create metadata for the file
                    var fileMetadata = new Google.Apis.Drive.v3.Data.File
                    {
                        Name = Path.GetFileName(zipFilePath),
                        Parents = new List<string> { folderId }
                    };

                    // Create the file upload request
                    FilesResource.CreateMediaUpload request;
                    using (var fileStream = new FileStream(zipFilePath, FileMode.Open, FileAccess.Read))
                    {
                        request = service.Files.Create(fileMetadata, fileStream, GetMimeType(zipFilePath));
                        request.Fields = "id";

                        // Execute the upload request
                        var uploadResponse = request.Upload();

                        // Check if the upload was successful
                        if (uploadResponse != null)
                        {
                            var uploadedFile = request.ResponseBody;
                            Console.WriteLine($"BAK file '{bakFile}' uploaded successfully as ZIP. File ID: {uploadedFile.Id}");


                            fileStream.Dispose();
                            // Delete the zip file from the current location
                            File.Delete(zipFilePath);
                        }
                        else
                        {
                            Console.WriteLine($"Failed to upload the BAK file '{bakFile}' as ZIP.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        // Function to get MIME type based on file extension
        private static string GetMimeType(string fileName)
        {
            string mimeType;
            if (Path.GetExtension(fileName).ToLower() == ".zip")
            {
                mimeType = "application/zip";
            }
            else
            {
                mimeType = "application/octet-stream";
            }
            return mimeType;
        }
        public static void UpdateProcessStatus(int JobId, Guid sessionId, DateTime? startTime, DateTime? endTime, string status)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionStrings))
            {
                SqlCommand cmd = new SqlCommand("CreateUpdateJobStatus", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("JobId ", JobId);
                cmd.Parameters.AddWithValue("SessionId", sessionId);
                cmd.Parameters.AddWithValue("StartTime", startTime);
                cmd.Parameters.AddWithValue("EndTime", endTime);
                cmd.Parameters.AddWithValue("Status", status);
                connection.Open();
                cmd.ExecuteNonQuery();
            }
           
        }
    }
}
