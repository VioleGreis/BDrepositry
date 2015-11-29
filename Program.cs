using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace MainProcess
{
    class Program
    {
        static void Main(string[] args)
        {
            GetDMV getdmv = new GetDMV();
            InsertDMV insdmv = new InsertDMV();
            string queryString1 = "SELECT * FROM [dbo].[Persons] p WHERE p.BusinessEntityID>2;";
            string queryString2 = "SELECT p.BusinessEntityID FROM [dbo].[Persons] p WHERE p.BusinessEntityID>2;";
            string clearcache = "DBCC FREEPROCCACHE";                // getdmv.StartQuery(clearcache);
            Console.ForegroundColor = ConsoleColor.White;
            CreateTables.TablesCreate();
            getdmv.StartQuery(clearcache);

            // После этого запускать основой цикл по обработке запросов

            for (int j = 0; j < 2; j++)
            {
                for (int i = 0; i < 2; i++)
                {
                    getdmv.StartQuery(queryString1);
                    getdmv.StartQuery(queryString2);
                }
                getdmv.GetDMVinfo(queryString1, "SELECT", "*", "Persons", "WHERE");
                getdmv.GetDMVinfo(queryString2, "SELECT", "BusinessEntityID", "Persons", "WHERE");
                //getdmv.StartQuery(clearcache);
            }
            insdmv.InsertDMVinfoInAll(getdmv);
            insdmv.InsertDMVinfoInAvg();
            Console.WriteLine("Program Complete. Enter to exit");
            Console.ReadKey();
        }
    }
}
