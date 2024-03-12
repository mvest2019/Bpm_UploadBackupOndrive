using Microsoft.Extensions.Configuration;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace UploadBackupOnGoogleDrive
{
    internal class DataAcess
    {
        private static IConfigurationRoot Configuration { get; set; }

        public static string ConnectionString = System.Configuration.ConfigurationManager.AppSettings.Get("ConnectionStrings");
        public static string SQLCommandTimeout = System.Configuration.ConfigurationManager.AppSettings.Get("SQLCommandTimeout");

        public static int ExecuteSPNonQuery(string connectionString, string procName, ListDictionary procParameters)
        {
            int retVal;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(procName, connection))
                {
                    connection.Open();
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = Int32.Parse(SQLCommandTimeout);

                    if (procParameters != null)
                    {
                        foreach (DictionaryEntry entry in procParameters)
                        {
                            SqlParameter parm = new SqlParameter(entry.Key.ToString(), entry.Value);
                            parm.Direction = ParameterDirection.Input;
                            command.Parameters.Add(parm);
                        }
                    }

                    SqlParameter returnValue = new SqlParameter("@RETURN_VALUE", SqlDbType.Int);
                    returnValue.Direction = ParameterDirection.ReturnValue;
                    command.Parameters.Add(returnValue);

                    command.ExecuteNonQuery();

                    retVal = (int)returnValue.Value;
                }
            }

            return retVal;
        }


        /// <summary>
        /// Executes a stored procedure and retrieves a DataSet
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="procName"></param>
        /// <returns></returns>
        public static DataSet ExecuteSPSelect(string connectionString, string procName)
        {
            return ExecuteSPSelect(connectionString, procName, null);
        }

        /// <summary>
        /// Executes a stored procedure and retrieves a DataSet
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="procName"></param>
        /// <param name="procParameters"></param>
        /// <returns></returns>
        public static DataSet ExecuteSPSelect(string connectionString, string procName, ListDictionary procParameters)
        {
            DataSet results = new DataSet();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(procName, connection))
                {
                    connection.Open();
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = Int32.Parse(SQLCommandTimeout);
                    if (procParameters != null)
                    {
                        foreach (DictionaryEntry entry in procParameters)
                        {
                            command.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                        }
                    }

                    SqlDataAdapter adapter = new SqlDataAdapter(command);
                    adapter.Fill(results);
                }
            }
            return results;
        }  

    }
}
