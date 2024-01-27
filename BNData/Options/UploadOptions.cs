using BNLib.BN;
using BNLib.DB;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BNData.Options
{
    [Verb("upload", HelpText = "Upload klines.")]
    internal class UploadOptions
    {
        [Option('i', "input", Required = false, HelpText = "Input dir/file")]
        public string? _inputfile { get; set; }

        public int Run()
        {
            if (_inputfile == null)
            {
                Console.WriteLine("Please specify input file/dir");
                return -1;
            }
            else
            {
                return Upload(_inputfile);
            }
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

        private int Upload(string filepath)
        {
            if (File.Exists(filepath))
                return UploadFile(filepath);
            else if (Directory.Exists(filepath))
                return UploadDir(filepath);
            else
                Console.WriteLine($"File or dir {filepath} not exists");
            return -1;
        }
        private int UploadFile(string filepath)
        {
            if (!File.Exists(filepath))
            {
                Console.WriteLine($"File {filepath} not exists");
                return -1;
            }

            using (var fs = File.OpenRead(filepath))
            {
                // read lines
                string symbol = fs.Name.Split('\\').Last().Split('.').First();
                TextReader tr = new StreamReader(fs);
                string[] lines = tr.ReadToEnd().Split('\n');
                if (lines.Length == 0)
                    return 0;

                var klines = Utils.ParseKlinesCSV(lines);
                // upload
                var db = GetDB();
                db.InsertSpotTable(symbol, klines).Wait();
            }
            return 0;
        }
        private int UploadDir(string dirpath)
        {
            if (!Directory.Exists(dirpath))
            {
                Console.WriteLine($"Dir {dirpath} not exists");
                return -1;
            }

            var files = Directory.GetFiles(dirpath);
            foreach (var f in files)
            {
                UploadFile(f);
            }
            return 0;
        }
    }
}
