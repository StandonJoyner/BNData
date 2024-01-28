using Binance.Net.Enums;
using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using BNLib.Frame;
using CommandLine;
using Serilog;
using System.Configuration;

namespace BNData.Options
{
    [Verb("update", HelpText = "Update database.")]
    internal class UpdateOptions
    {
        private static readonly ILogger _logger = Serilog.Log.ForContext<UpdateOptions>();

        [Option('s', "symbols", Required = false, HelpText = "Coin symbols")]
        public string? _symbol { get; set; }

        [Option('i', "input", Required = false, HelpText = "Input dir/file")]
        public string? _inputfile { get; set; }

        [Option('b', "begdate", Required = false, HelpText = "Begin date")]
        public string? _begdate { get; set; }

        [Option('e', "enddate", Required = false, HelpText = "End date")]
        public string? _enddate { get; set; }

        [Option('o', "output", Required = false, HelpText = "Output dir name")]
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
                return UpdateDBAll();
            else if (_symbol != null)
                return UpdateDBSymbol(_symbol).Result;
            else
                return -1;
        }

        public int UpdateDBAll()
        {
            var db = GetDB();
            KlinesUpdate up = new KlinesUpdate();
            var result = ExchangeInfo.GetAllSymbols(MarketType.SPOT, true);
            result.Wait();
            var syms = result.Result;

            List<Task> tasks = new List<Task>();
            foreach (var s in syms)
            {
                var t = Task.Run(async () =>
                {
                    Log.Information($"Update {s}");
                    // await up.DownloadKlinesAll(db, MarketType.SPOT, s, KlineInterval.OneDay);
                });
                tasks.Add(t);
            }
            Task.WaitAll(tasks.ToArray());
            return 0;
        }

        public async Task<int> UpdateDBSymbol(string symbol)
        {
            var db = GetDB();
            string? strBegDate = ConfigurationManager.AppSettings["begin_date"];
            if (strBegDate == null)
                throw new Exception("Cannot find begin_date config");

            KlinesUpdate up = new KlinesUpdate();
            // await up.UpdateKlinesAll(db, MarketType.SPOT, symbol, KlineInterval.OneDay);
            return 0;
        }
    }
}
