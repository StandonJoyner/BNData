using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using BNLib.DB;
using Newtonsoft.Json;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860
using ListTable = System.Collections.Generic.List<System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.List<object>>>;
using System.Text;
using System.IO.Compression;
using BNAPI.Common;
using CryptoExchange.Net.CommonObjects;
using System.Threading;
using Microsoft.VisualBasic;
using Microsoft.AspNetCore.Http;

namespace BNAPI.Controllers
{
    struct DHParams
    {
        public string[] symbols;
        public string[] indis;
        public DateTime tbeg;
        public DateTime tend;
        public string? ext;
    }
    struct RequestStatus
    {
        public int err_code;
        public string err_msg;
    }
    [Route("v1/BN/[controller]")]
    [ApiController]
    public class KlinesController : ControllerBase
    {
        private readonly ILogger<KlinesController> _logger;
        static SemaphoreSlim _semaphore = new SemaphoreSlim(90);
        public KlinesController(ILogger<KlinesController> logger)
        {
            _logger = logger;
        }

        [HttpGet("DataHistory")]
        public async Task<IActionResult> GetDH(string symbols, string indis, string tbeg, string tend, string? ext = null)
        {
            _logger.LogInformation($"GetDS {RemoteIP()} {symbols} {indis} {tbeg} {tend} {ext}");

            try
            {
                await _semaphore.WaitAsync();
                try
                {
                    DHParams dhParams = CheckAndParse(symbols, indis, tbeg, tend, ext);
                    var result = await GetDHAll(dhParams);

                    var json = JsonConvert.SerializeObject(result);
                    return Ok(json);
                }
                catch (Exception e)
                {
                    _logger.LogError(e.Message);
                    return BadRequest(e.Message);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        DHParams CheckAndParse(string symbols, string indis, string tbeg, string tend, string? ext)
        {
            if (symbols == null || symbols.Length == 0)
            {
                throw new Exception("Missing symbols");
            }
            if (indis == null || indis.Length == 0)
            {
                throw new Exception("Missing indicators");
            }
            DHParams dhParams = new DHParams();
            var symbol = symbols.Split(',');
            dhParams.symbols = new string[symbol.Length];
            for (int i = 0; i < symbol.Length; i++)
            {
                var assets = symbol[i].Split('/');
                if (assets.Length != 2)
                {
                    throw new Exception($"Invalid symbol {symbol[i]}");
                }
                dhParams.symbols[i] = assets[0] + assets[1];
            }
            dhParams.indis = indis.Split(',');

            if (!DateTime.TryParse(tbeg, out DateTime tbegDate))
            {
                throw new Exception("Invalid begin date");
            }
            if (!DateTime.TryParse(tend, out DateTime tendDate))
            {
                throw new Exception("Invalid end date");
            }
            if (tbegDate >= tendDate)
            {
                throw new Exception("Begin date must be earlier than end date");
            }
            DateTime.SpecifyKind(tbegDate, DateTimeKind.Utc);
            DateTime.SpecifyKind(tendDate, DateTimeKind.Utc);
            dhParams.tbeg = tbegDate;
            dhParams.tend = tendDate;
            dhParams.ext = ext;
            return dhParams;
        }

        private async Task<Dictionary<string, object>> GetDHAll(DHParams dhParams)
        {
            var columns = GetSQLColumns(dhParams.indis, dhParams.tbeg, dhParams.tend, dhParams.ext);

            try
            {
                int cells = 0;
                var dict = new List<KeyValuePair<string, ListTable>>(dhParams.symbols.Length);
                foreach (var s in dhParams.symbols)
                {
                    var ds = await GetDHOne(s, columns, dhParams.tbeg, dhParams.tend);
                    cells += ds[0].Value.Count;
                    dict.Add(new KeyValuePair<string, ListTable>(s, ds));
                }
                _logger.LogInformation($"GetDS cells {cells}");

                var result = new Dictionary<string, object>();
                result["data"] = dict;
                result["status"] = new RequestStatus { err_code = 0, err_msg = "OK" };

                await RecordLog(dhParams, cells);
                return result;
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                var result = new Dictionary<string, object>();
                result["data"] = null;
                result["status"] = new RequestStatus { err_code = 1, err_msg = e.Message };
                return result;
            }
        }

        private async Task<ListTable> GetDHOne(string symbol, string columns, DateTime tbeg, DateTime tend)
        {
            var db = GetDB();
            var table = await db.QueryDataHistory(symbol, columns, tbeg, tend);
            return Convert(table);
        }

        private ListTable Convert(DataTable dt)
        {
            var dict = new ListTable(dt.Columns.Count);
            int i = 0;
            foreach (DataColumn column in dt.Columns)
            {
                // List效率高，而且可以保持顺序
                var col = new List<object>(dt.Rows.Count);
                foreach (DataRow row in dt.Rows)
                {
                    col.Add(row[column]);
                }
                dict.Add(new KeyValuePair<string, List<object>>(column.ColumnName, col));
                i++;
            }
            return dict;
        }

        private string GetSQLColumns(string[] indis, DateTime tbeg, DateTime tend, string? ext)
        {
            string[] legals = { "open", "high", "low", "close", "volume" };
            foreach (var i in indis)
            {
                if (!legals.Contains(i))
                {
                    throw new ArgumentException($"Invalid indicator {i}");
                }
            }
            StringBuilder sql = new StringBuilder();
            sql.Append("open_time");
            foreach (var i in indis)
            {
                sql.Append($", {i}");
            }
            return sql.ToString();
        }

        PgDB GetDB()
        {
            // 要先创建数据库: CREATE DATABASE bndata;
            PgDB db = new PgDB();
            db.Connect(DBConfig.Instance.Host,
                DBConfig.Instance.Port,
                DBConfig.Instance.User,
                DBConfig.Instance.Password,
                DBConfig.Instance.DB);
            return db;
        }

        string RemoteIP()
        {
            var ip = HttpContext.Connection.RemoteIpAddress;
            if (ip != null)
                return ip.ToString();
            else
                return "unknown";
        }

        async Task RecordLog(DHParams dhParams, int cells)
        {
            var symbols = string.Join(',', dhParams.symbols);
            var indis = string.Join(',', dhParams.indis);
            var strParam = symbols + " " + indis;

            var db = GetDB();
            await db.RecordLog(RemoteIP(), strParam, cells, "");
        }
    }
}
