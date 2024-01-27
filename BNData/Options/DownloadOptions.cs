using Binance.Net.Enums;
using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using BNLib.Frame;
using CommandLine;
using Serilog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BNData.Options
{
    [Verb("download", HelpText = "Download klines.")]
    internal class DownloadOptions
    {
        private static readonly ILogger _logger = Serilog.Log.ForContext<UpdateOptions>();

        [Option('s', "symbols", Required = false, HelpText = "Coin symbols")]
        public string? _symbols { get; set; }

        public int Run()
        {
            if (_symbols == null)
            {
                // 下载所有缺失的数据
                DownloadAll().Wait();
                return 0;
            }
            else
            {
                // 下载指定的数据
                var syms = _symbols.Split(',');
                DownloadSymbols(syms).Wait();
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

        public async Task<int> DownloadAll()
        {
            var result = ExchangeInfo.GetAllSymbols(MarketType.SPOT, true);
            result.Wait();
            var syms = result.Result;

            return await DownloadSymbols(syms);
        }

        public async Task<int> DownloadSymbols(IEnumerable<string> syms)
        {
            var db = GetDB();
            string? strBegDate = ConfigurationManager.AppSettings["begin_date"];
            if (strBegDate == null)
                throw new Exception("Cannot find begin_date config");
            var tbegDate = DateTime.Parse(strBegDate);

            KlinesUpdate up = new KlinesUpdate();

            // make dir to save klines
            DateTime now = DateTime.Now;
            string outputDir = "klines";
            outputDir = Path.Combine(outputDir, now.ToString("yyyyMMdd_hh_mm"));

            if (!Directory.Exists(outputDir))
                Directory.CreateDirectory(outputDir);

            List<Task> tasks = new List<Task>();
            foreach (var s in syms)
            {
                var t = Task.Run(async () =>
                {
                    // create new file
                    var newfile = Path.Combine(outputDir, $"{s}.csv");
                    if (File.Exists(newfile))
                        File.Delete(newfile);
                    using (TextWriter tw = new StreamWriter(newfile))
                    {
                        await up.DownloadKlinesAll(db, MarketType.SPOT, s, KlineInterval.OneDay, tbegDate, tw);
                    }
                });
                tasks.Add(t);
                if (tasks.Count > 30)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            return 0;
        }
    }
}
