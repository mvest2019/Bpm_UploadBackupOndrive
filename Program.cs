using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
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
            UploadLatestBackupFileToGoogleDrive(credentialsFilePath, folderId, folderPath);
            UpdateProcessStatus(38, sessionid, null, DateTime.Now, "FINISHED");
            Console.WriteLine("End");

        }

        public static void UploadLatestBackupFileToGoogleDrive(string credentialsFilePath, string folderId, string folderPath)
        {
            try
            {
                // Load credentials from the JSON key file
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

                // Get the latest BAK file in the folder
                string latestBakFile = GetLatestBackupFile(folderPath);

                if (latestBakFile != null)
                {
                    // Convert BAK file to ZIP format
                    string zipFilePath = Path.Combine(Path.GetDirectoryName(latestBakFile), Path.GetFileNameWithoutExtension(latestBakFile) + ".zip");
                    using (var zipArchive = ZipFile.Open(zipFilePath, ZipArchiveMode.Create))
                    {
                        zipArchive.CreateEntryFromFile(latestBakFile, Path.GetFileName(latestBakFile));
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
                            Console.WriteLine($"BAK file '{latestBakFile}' uploaded successfully as ZIP. File ID: {uploadedFile.Id}");


                            fileStream.Dispose();
                            // Delete the zip file from the current location
                               File.Delete(zipFilePath);
                        }
                        else
                        {
                            Console.WriteLine($"Failed to upload the BAK file '{latestBakFile}' as ZIP.");
                        }
                    }
                }
                else
                {
                    Console.WriteLine("No .bak file found in the folder.");
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
            return Path.GetExtension(fileName).ToLower() == ".zip" ? "application/zip" : "application/octet-stream";
        }

        // Function to get the latest .bak file in the folder
        private static string GetLatestBackupFile(string folderPath)
        {
            string[] bakFiles = Directory.GetFiles(folderPath, "*.bak");
            if (bakFiles.Length > 0)
            {
                Array.Sort(bakFiles, (a, b) => File.GetLastWriteTime(b).CompareTo(File.GetLastWriteTime(a)));
                return bakFiles[0]; // Return the latest file
            }
            else
            {
                return null;
            }
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
