using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Data.SqlClient;
namespace MainProcess
{
    class GetDMV
    {
        public struct TType
        {
            public int CPUTime;
            public string QueryText;
            public string ObjType;
            public string query_hash;
            public int ExecCount;
            public string QueryPlan;
            public byte[] QueryHash;
            public string temp;
        }
        public List<TType> InfoStruct = new List<TType>();
        public void GetDMVinfo(string GetQueryInfo,string Method, string parametrs, string Table, string Extra)
        {
            int StructCount = 0;
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            string DMVString = "SELECT query_hash, st.text,total_worker_time,cp.objtype,qs.execution_count,qp.query_plan,qs.query_hash FROM sys.dm_exec_query_stats qs " +
                              "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st " +
                              "CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp " +
                              "JOIN sys.dm_exec_cached_plans cp " +
                              "ON cp.plan_handle = qs.plan_handle " +
                              "WHERE  (st.text like'%" + Method + "%%"+ parametrs+"%%" + Table + "%%" + Extra + "%' AND st.text NOT like '%st.text%')";

            //Console.WriteLine(DMVString);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                Console.WriteLine("GetDMVInfo Connection Success \n");
                SqlCommand command = new SqlCommand(DMVString, connection);
                try
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    Console.WriteLine("Select info from DMV");
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    while (reader.Read())
                    {
                        TType newTType = new TType();
                        Console.WriteLine("--------------->  New Query has been searshed  <------------ \n");
                        Console.WriteLine("\t{0}\t{1}\t{2}\t{3}", reader[0], reader[1], reader[2], reader[3]);
                        Console.WriteLine("");
                        newTType.query_hash = reader[0].ToString();
                        newTType.QueryText = reader[1].ToString();
                        newTType.CPUTime = Int32.Parse(reader[2].ToString());
                        newTType.ObjType = reader[3].ToString();
                        newTType.ExecCount = Int32.Parse(reader[4].ToString());
                        newTType.QueryPlan = reader[5].ToString();
                        newTType.QueryHash = (byte[])reader[6];
                        newTType.temp = reader[6].ToString();
                        if (newTType.ObjType != "Adhoc")
                        { 
                            InfoStruct.Add(newTType);
                            StructCount++;
                        }
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("GetDMVinfo: Status -----> Complete \n");
                    Console.ForegroundColor = ConsoleColor.White;
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Не удалось получить DMVinfo \n");
                }
            }
        }
        public void StartQuery(string GetQueryInfo)
        {
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                Console.WriteLine("StartQuery {0} connection Success \n", GetQueryInfo.Substring(0,10));
                SqlCommand command = new SqlCommand(GetQueryInfo, connection);
                try
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                      //  Console.WriteLine("\t{0}", reader[0]);
                    }
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("QueryPost: Status -----> Complete \n");
                    Console.ForegroundColor = ConsoleColor.White;
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Не удалось выполнить Query \n");
                }
            }
        }

    }
}
