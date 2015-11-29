﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
namespace MainProcess
{
    class InsertDMV
    {
        List<GetDMV.TType> TTypeList = new List<GetDMV.TType>();
        public void InsertDMVinfoInAll(GetDMV getdmv)
        {
            int BoolConnect = 0;
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            foreach (var s in getdmv.InfoStruct)
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    long sum = 0;
                    for (int i = 0; i < s.QueryHash.Count(); i++)
                    {
                        sum = (int)s.QueryHash[0] * (int)(Math.Pow(16, 14));
                    }
                    string SetQuery = "INSERT INTO dbo.AllQueryTable( CPUTime,ExecTime,QueryText, ObjType, ExecCount, query_plan, query_hash ) " +
                                      "VALUES ( " +
                                          "" + s.CPUTime.ToString() + "," +
                                          "" + s.CPUTime.ToString() + "," +
                                          "'" + s.QueryText + "'," +
                                          "N'" + s.ObjType + "'," +
                                          "" + s.ExecCount + "," +
                                          "'"+ null +"', " +
                                          "" + sum + " " +
                                          ")";
                    Console.WriteLine(SetQuery);
                    if (BoolConnect == 0)
                    {
                        Console.WriteLine("Insert into All Connection Success \n");
                        BoolConnect++;
                    }
                    SqlCommand command = new SqlCommand(SetQuery, connection);
                    try
                    {
                        connection.Open();
                        SqlDataReader reader = command.ExecuteReader();
                        while (reader.Read())
                        {

                        }
                        Console.WriteLine("Execute query {0}", SetQuery.Substring(80,80));
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("ExecuteQuery: Status -----> Complete \n");
                        Console.ForegroundColor = ConsoleColor.White;
                        reader.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Не удалось выполнить InsertDMVinfoInAll \n");
                        Console.ForegroundColor = ConsoleColor.White;
                    }

                }
            }
        }
        void SelectStates()
        {
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string SetQuery = "SELECT * FROM dbo.AllQueryTable";
                Console.WriteLine("Insert Connection Success \n");
                SqlCommand command = new SqlCommand(SetQuery, connection);
                try
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        GetDMV.TType newTType = new GetDMV.TType();
                        newTType.query_hash = null;
                        newTType.CPUTime = Int32.Parse(reader[1].ToString());
                        newTType.QueryText = reader[3].ToString();
                        newTType.ObjType = reader[4].ToString();
                        newTType.ExecCount = Int32.Parse(reader[5].ToString());
                        newTType.QueryPlan = reader[6].ToString();
                        TTypeList.Add(newTType);
                    }
                    Console.WriteLine("Execute 'Select in AllTable' ");
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Stats count has been grabed -----> {0}\n",TTypeList.Count());
                    Console.ForegroundColor = ConsoleColor.White;
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Не удалось выполнить SelectStates \n");
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
        public void InsertDMVinfoInAvg()   //ЖУТКАЯ НЕОПТИМИЗАЦИЯ
        {
            string connectionString = @"Data Source=(local)\SERVER2012;
                            Initial Catalog=AdventureWorks2012;
                            Integrated Security=True";
            string DMVString ="INSERT INTO[AvgQueryTable] ([QueryText], [CPUTime], [ExecTime], [Median], [Disp], [Count], [query_plan]) " +
                              "SELECT DISTINCT CAST(aqt.QueryText as Varchar(MAX)), " +
                              "SUM(aqt.CPUTime)/SUM(ExecCount), SUM(aqt.CPUTime)/SUM(ExecCount), 0, 0, SUM(ExecCount), aqt.query_plan " +
                              "FROM dbo.AllQueryTable aqt " +
                              "GROUP BY CAST(aqt.QueryText as Varchar(MAX)) ";
            Console.WriteLine(DMVString);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                Console.WriteLine("Avg Insert Connection Success \n");
                SqlCommand command = new SqlCommand(DMVString, connection);
                try
                {
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    Console.WriteLine("Execute Insert into Avg query");
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    while (reader.Read())
                    {
                    }
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("InsertInAvg: Status -----> Complete \n");
                    Console.ForegroundColor = ConsoleColor.White;
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Не удалось заполнить Avg \n");
                }
            }

            ///////////////////////Рассчёт Медиан//////////////////////////////////////////////


            SelectStates();
            List<string> uniqueText = new List<String>();
            foreach (var e in TTypeList)
            {
                if (!uniqueText.Contains(e.QueryText))
                {
                    uniqueText.Add(e.QueryText);
                }
            }
            List<List<int>> Median = new List<List<int>>();
            List<List<int>> ExecCount = new List<List<int>>();
            for (int i = 0; i < uniqueText.Count(); i++)
            {
                Median.Add(new List<int>());
                ExecCount.Add(new List<int>());
            }

            foreach (var e in TTypeList)
            {
                int count = 0;
                while (count < uniqueText.Count())
                {
                    if (e.QueryText == uniqueText[count])
                    {
                        Median[count].Add(e.CPUTime);
                        ExecCount[count].Add(e.ExecCount);
                        //Console.WriteLine(e.ExecCount);
                    }
                    count++;
                }
            }
            for (int i = 0; i < Median.Count(); i++)
            {
                Median[i].Sort();
            }

            ///////////////////////////////конец рассчёта медиан//////////////////////////////////////////
            double[] Mat = new double[Median.Count];
            double[] Disp = new double[Median.Count];
            for (int i=0; i< Median.Count();i++)        //рассчёт матожидания
            {
                int count=0;
                for (int j = 0; j < Median[i].Count(); j++)
                {
                    Mat[i] += Median[i][j];
                    count++;           
                }
                Mat[i] = Mat[i] / count;
            }

            for (int i = 0; i < Median.Count(); i++)        //рассчёт дисперсии
            {
                int count = 0;
                for (int j = 0; j < Median[i].Count(); j++)
                {
                    Disp[i] +=Math.Pow(((Median[i][j]-Mat[i])),2);
                    count++;
                }
                Disp[i] = Disp[i] / count;
                Disp[i] = Math.Sqrt(Disp[i]);            }


            for (int i=0;i<uniqueText.Count();i++)
            {
               DMVString = "UPDATE dbo.AvgQueryTable " +
                            "SET AvgQueryTable.Median = " + Median[i][Median[i].Count / 2]/ExecCount[i][ExecCount[i].Count/2] + ", " +
                            "AvgQueryTable.Disp = " + (int)Disp[i] / ExecCount[i][ExecCount[i].Count / 2] + " " +
                            "WHERE CAST(QueryText as Varchar(MAX))= '" + uniqueText[i] + "'";
                //Console.WriteLine(DMVString);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    Console.WriteLine("Avg Update connection Success \n");
                    SqlCommand command = new SqlCommand(DMVString, connection);
                    try
                    {
                        connection.Open();
                        SqlDataReader reader = command.ExecuteReader();
                        Console.WriteLine("Execute Avg Update query");
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        while (reader.Read())
                        {
                        }
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("UpdateMedianInAvg: Status -----> Complete \n");
                        Console.ForegroundColor = ConsoleColor.White;
                        reader.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Не удалось сделать апдейт Avg \n");
                    }
                }
            }


        }

        public void InsertDMVinfoInBadQueries()
        {

        }
    }
}
