using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace MainProcess
{
    class CreateTables
    {

        public static void TablesCreate()
        {
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            SqlCommand command;
            SqlDataReader reader;
            string queryString =  "CREATE TABLE [dbo].[AllQueryTable] ( ID  int NOT NULL identity(1,1),CPUTime bigint NULL,ExecTime bigint NULL,QueryText Text NULL, ObjType nvarchar(16) NULL, ExecCount int NULL, query_plan XML NULL,query_hash binary(8) NULL CONSTRAINT ID_AllQueryConstraint PRIMARY KEY(ID) ) ";
            string queryString2 = "DROP TABLE AvgQueryTable " +
                                  "CREATE TABLE [dbo].[AvgQueryTable] ( ID  int NOT NULL identity(1,1),QueryText Text NULL, CPUTime bigint NULL,ExecTime bigint NULL,Median float NULL,Disp float NULL,Count int NULL,query_plan XML NULL,query_hash binary(8) NULL CONSTRAINT ID_AvgQueryConstraint PRIMARY KEY(ID) ) ";
            string queryString3 = "CREATE TABLE [dbo].[AvgTimeQueryTable] ( ID  int NOT NULL identity(1,1) ,QueryText Text NULL, CPUTime bigint NULL,ExecTime bigint NULL,Median int NULL,Disp int NULL,Count int NULL,query_plan XML NULL, CONSTRAINT ID_AvgTimeQueryConstraint PRIMARY KEY(ID) ) ";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                Console.WriteLine("Create Table connection Success \n");
                command = new SqlCommand(queryString, connection);
                // command.Parameters.AddWithValue("@pricePoint", paramValue);
                try
                {
                    connection.Open();
                    reader = command.ExecuteReader();
                    //while (reader.Read())
                    //{
                    //      Console.WriteLine("\t{0}\t{1}\t{2}", reader[0], reader[1], reader[2]);
                    //    //html.Append(String.Format("<tr><td>{0}</td><td>{1}</td><td>{2}</td>", reader[0], reader[1], reader[2]));
                    //}
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("AllTable Create \n");
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Таблица ALL не была создана");
                    Console.WriteLine("Возможно она уже сущесвтует \n");
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                command = new SqlCommand(queryString2, connection);
                try
                {
                    connection.Open();
                    reader = command.ExecuteReader();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("AvgTable Create \n");
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Таблица AVG не была создана");
                    Console.WriteLine("Возможно она уже сущесвтует \n");
                }
            }
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                command = new SqlCommand(queryString3, connection);
                try
                {
                    connection.Open();
                    reader = command.ExecuteReader();
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("AvgTable Create \n");               
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Таблица AVGTime не была создана");
                    Console.WriteLine("Возможно она уже сущесвтует \n");
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
