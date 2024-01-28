using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BNData.Options
{
    [Verb("symbols", HelpText = "Update symbols info.")]
    internal class SymbolOptions
    {
        public int Run()
        {
            var result = ExchangeInfo.GetAllSymbols(MarketType.SPOT, true);
            result.Wait();
            var syms = result.Result;
            var db = GetDB();
            foreach (var sym in syms)
            {
                var sr = ExchangeInfo.GetSymbol(MarketType.SPOT, sym);
                sr.Wait();
                var info = sr.Result;
                if (info == null)
                    continue;
                db.UpdateSymbolInfo(info).Wait();
            }

            return 0;
        }

        PgDB GetDB()
        {
            string? host = ConfigurationManager.AppSettings["dbhost"];
            string? port = ConfigurationManager.AppSettings["dbport"];
            string? user = ConfigurationManager.AppSettings["dbuser"];
            string? passwd = ConfigurationManager.AppSettings["dbpasswd"];
            if (host == null || port == null || user == null || passwd == null)
                throw new Exception("Cannot find db config");

            // 要先创建数据库: CREATE DATABASE bndata;
            PgDB db = new PgDB();
            db.Connect(host, port, user, passwd, "bndata");
            return db;
        }
    }
}
