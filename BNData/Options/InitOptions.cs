using Binance.Net.Enums;
using BNLib.BN;
using BNLib.Enums;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BNLib.DB;

namespace BNData.Options
{
    [Verb("init", HelpText = "Initialize database.")]
    internal class InitOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        public int Run()
        {
            testInit();
            return 0;
        }
        void testInit()
        {
            PgDB db = new PgDB();
            db.InitDB("postgres", "123456", "bndata")
                .Wait();
        }
    }
}
