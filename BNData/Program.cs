﻿// See https://aka.ms/new-console-template for more information

using Binance.Net.Enums;
using BNLib.Frame;
using BNLib.Enums;
using log4net;
using CommandLine;
using BNData.Options;
using Serilog;

namespace BNData
{
    class Program
    {
        [Verb("add", HelpText = "Add file contents to the index.")]
        class AddOptions
        {
            //normal options here
            [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
            public bool Verbose { get; set; }
            public int Run()
            {
                throw new System.NotImplementedException();
            }
        }

        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.File("logs/myapp.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            Parser.Default.ParseArguments<InitOptions, UpdateOptions>(args)
                .MapResult(
                    (InitOptions opts) => opts.Run(),
                    (UpdateOptions opts) => opts.Run(),
                    errs => 1
                );
        }

        void test2()
        {
            DateTime begDate = new DateTime(2023, 11, 5);
            DateTime endDate = new DateTime(2023, 12, 7);
            //KLineDownload.DownloadMonthlyKLines(MarketType.SPOT, "BTCUSDT", KlineInterval.OneMonth, begDate, endDate).Wait();

            KlinesUpdate up = new KlinesUpdate();
            up.GetKlines(MarketType.SPOT, "BTCUSDT", KlineInterval.OneDay, begDate, endDate).Wait();
            //up.UpdateKlines(MarketType.SPOT, "BTCUSDT", begDate, endDate).Wait();
        }
    }
}
