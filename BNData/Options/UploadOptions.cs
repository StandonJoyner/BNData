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
                string strContent = tr.ReadToEnd();
                if (strContent.Length == 0)
                    return 0;
                string[] lines = strContent.Split('\n');
                if (lines.Length == 0)
                    return 0;

                var klines = Utils.ParseKlinesCSV(lines);
                // upload
                var db = GetDB();
                var res = db.InsertSpotTable(symbol, klines);
                res.Wait();
                bool succ = res.Result;
                if (!succ)
                {
                    Console.WriteLine($"Upload {filepath} failed");
                    return -1;
                }
                else
                {
                    Console.WriteLine($"Upload {filepath} success");
                    return 0;
                }
            }
        }
        private int UploadDir(string dirpath)
        {
            if (!Directory.Exists(dirpath))
            {
                Console.WriteLine($"Dir {dirpath} not exists");
                return -1;
            }

            // 已成功上传的Symbol
            HashSet<string> existSyms = new HashSet<string>();
            string succFile = Path.Combine(dirpath, "000000.txt");
            if (File.Exists(succFile))
            {
                string[] lines = File.ReadAllLines(succFile);
                foreach (var line in lines)
                {
                    if (string.IsNullOrEmpty(line))
                        continue;
                    existSyms.Add(line);
                }
            }

            var files = Directory.GetFiles(dirpath);
            foreach (var f in files)
            {
                if (f.EndsWith(".csv"))
                {
                    // 过滤掉已上传成功的文件
                    if (existSyms.Contains(f))
                        continue;

                    int succ = UploadFile(f);
                    if (succ == 0)
                    {
                        File.AppendAllText(succFile, f + "\n");
                    }
                }
            }
            return 0;
        }
    }
}
