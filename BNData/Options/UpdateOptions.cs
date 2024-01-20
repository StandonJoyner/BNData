using Binance.Net.Enums;
using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using BNLib.Frame;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BNData.Options
{
    [Verb("update", HelpText = "Update database.")]
    internal class UpdateOptions
    {
        [Option('s', "symbols", Required = false, HelpText = "Coin symbols")]
        public string? _symbol { get; set; }

        [Option('i', "input", Required = false, HelpText = "Input symbol file")]
        public string? _inputfile { get; set; }

        [Option('b', "begdate", Required = false, HelpText = "Begin date")]
        public string? _begdate { get; set; }

        [Option('e', "enddate", Required = false, HelpText = "End date")]
        public string? _enddate { get; set; }

        [Option('o', "output", Required = false, HelpText = "Output file name")]
        public string? _output { get; set; }

        public int Run()
        {
            if (_output == null)
                return UpdateDB();
            else
                return UpdateFile();
        }

        public int UpdateFile()
        {
            return -1;
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

        public int UpdateDB()
        {
            if (_symbol == null && _inputfile == null)
                return UpdateDBAll().Result;
            else if (_symbol != null)
                return UpdateDBSymbol(_symbol).Result;
            else
                return -1;
        }

        public async Task<int> UpdateDBAll()
        {
            var db = GetDB();
            KlinesUpdate up = new KlinesUpdate();
            var syms = await ExchangeInfo.GetAllSymbols(MarketType.SPOT, true);
            foreach (var s in syms)
            {
                await up.UpdateKlinesAll(db, MarketType.SPOT, s, KlineInterval.OneDay);
            }
            return 0;
        }

        public async Task<int> UpdateDBSymbol(string symbol)
        {
            var db = GetDB();
            KlinesUpdate up = new KlinesUpdate();
            await up.UpdateKlinesAll(db, MarketType.SPOT, symbol, KlineInterval.OneDay);
            return 0;
        }

        void testMerge()
        {
            KlinesUpdate up = new KlinesUpdate();
            up.GetLackKlines(MarketType.SPOT, "BTCUSDT", KlineInterval.OneDay, new DateTime(2021, 1, 1), new DateTime(2023, 12, 30)).Wait();
        }

    }
}
