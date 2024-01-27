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
using System.Configuration;

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
            string? host = ConfigurationManager.AppSettings["dbhost"];
            string? port = ConfigurationManager.AppSettings["dbport"];
            string? user = ConfigurationManager.AppSettings["dbuser"];
            string? passwd = ConfigurationManager.AppSettings["dbpasswd"];
            if (host == null || port == null || user == null || passwd == null)
                throw new Exception("Cannot find db config");

            // 安装postgresql
            // 修改密码 ALTER USER postgres WITH PASSWORD '123456';
            // 修改端口
            // 安装timescaledb
            // 配置timescaledb-tune
            // 要先创建数据库: CREATE DATABASE bndata;
            // 允许远程连接
            // 启用timescaledb
            PgDB db = new PgDB();
            db.InitDB(host, port, user, passwd, "bndata")
                .Wait();
        }
    }
}
