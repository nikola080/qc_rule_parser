using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresConn.Services
{
    public class PostgresConn
    {
        private static bool connStatus = false;
        private static string connectionString = "Host=173.212.248.194;Port=5450;Username=postgres;Password=M@p2io!pS;Database=eionet";
        private static NpgsqlConnection connection = null;
        public static bool openConnection()
        {
            
            if(connection == null)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                }
                catch (Exception ex)
                {
                    connection = null;
                    Console.WriteLine("Error creating connection: " + ex.Message);
                }
            }

            if (connection != null && connStatus == false)
            {
                connection.Open();
                connStatus = true;
            }

            return connStatus;
        }
        public static void closeConnection()
        {
            if (connection != null && connStatus == true)
            {
                connection.Close();
                connStatus = false;
            }
        }
        public static NpgsqlConnection getConnection()
        {
            if(connection == null)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                }
                catch (Exception ex)
                {
                    connection = null;
                    Console.WriteLine("Error creating connection: " + ex.Message);
                }
            }
            return connection;
        }
    }
}
