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

namespace BNAPI.Controllers
{
    struct RequestStatus
    {
        public int    err_code;
        public string err_msg;
    }
    [Route("v1/BN/[controller]")]
    [ApiController]
    public class KlinesController : ControllerBase
    {
        private readonly ILogger<KlinesController> _logger;
        public KlinesController(ILogger<KlinesController> logger)
        {
            _logger = logger;
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
        [HttpGet("dateseries")]
        public async Task<IActionResult> GetDS(string symbols, string indis, string tbeg, string tend, string? ext = null)
        {
            _logger.LogInformation($"GetDS {symbols} {indis} {tbeg} {tend} {ext}");
            var symbol = symbols.Split(',');
            var indiAry = indis.Split(',');
            if (!DateTime.TryParse(tbeg, out DateTime tbegDate))
            {
                return BadRequest("Invalid tbeg parameter");
            }
            if (!DateTime.TryParse(tend, out DateTime tendDate))
            {
                return BadRequest("Invalid tend parameter");
            }
            var columns = GetSQLColumns(indiAry, tbegDate, tendDate, ext);

            int cells = 0;
            var dict = new List<KeyValuePair<string, ListTable>>(symbol.Length);
            foreach (var s in symbol)
            {
                var assets = s.Split('/');
                if (assets.Length != 2)
                {
                    return BadRequest("Invalid symbol parameter");
                }
                var ds = await GetDSOne(assets[0] + assets[1], columns, tbegDate, tendDate);
                cells += ds[0].Value.Count;
                dict.Add(new KeyValuePair<string, ListTable>(s, ds));
            }
            _logger.LogInformation($"GetDS cells {cells}");
            var result = new Dictionary<string, object>();
            result["data"] = dict;
            result["status"] = new RequestStatus { err_code = 0, err_msg = "OK" };

            var json = JsonConvert.SerializeObject(result);
            return Ok(json);
        }

        private string GetSQLColumns(string[] indis, DateTime tbeg, DateTime tend, string? ext)
        {
            string[] legals= {"open", "high", "low", "close", "volume"};
            foreach (var i in indis)
            {
                if (!legals.Contains(i))
                {
                    throw new ArgumentException($"Invalid indicator {i}");
                }
            }
            StringBuilder sql = new StringBuilder();
            sql.Append("date");
            foreach (var i in indis)
            {
                sql.Append($", {i}");
            }
            return sql.ToString();
        }

        private async Task<ListTable> GetDSOne(string symbol, string columns, DateTime tbeg, DateTime tend)
        {
            string strBegin = tbeg.ToString("yyyy-MM-dd");
            string strEnd = tend.ToString("yyyy-MM-dd");
            var sql = $"SELECT {columns} FROM spot_klines_1d " +
                $"WHERE symbol='{symbol}' AND" +
                $"      date >= '{strBegin}' and date <= '{strEnd}'" +
                $";";
            var db = GetDB();
            var table = await db.QueryDataAsync(sql);
            return Convert(table);
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
    }
}
