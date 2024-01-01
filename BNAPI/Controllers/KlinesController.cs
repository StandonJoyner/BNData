using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using BNLib.DB;
using Newtonsoft.Json;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860
using DictTable = System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<object>>;

namespace BNAPI.Controllers
{
    [Route("v1/BN/[controller]")]
    [ApiController]
    public class KlinesController : ControllerBase
    {
        private DictTable Convert(DataTable dt)
        {
            var dict = new DictTable();
            foreach (DataColumn column in dt.Columns)
            {
                dict[column.ColumnName] = new List<object>();
            }
            foreach (DataRow row in dt.Rows)
            {
                foreach (DataColumn column in dt.Columns)
                {
                    dict[column.ColumnName].Add(row[column]);
                }
            }
            return dict;
        }
        [HttpGet("dateseries")]
        public async Task<IActionResult> GetDS(string symbols, string indis, string tbeg, string tend, string? ext = null)
        {
            var symbol = symbols.Split(',');
            var indi = indis.Split(',');
            if (!DateTime.TryParse(tbeg, out DateTime tbegDate))
            {
                return BadRequest("Invalid tbeg parameter");
            }
            if (!DateTime.TryParse(tend, out DateTime tendDate))
            {
                return BadRequest("Invalid tend parameter");
            }
            var dict = new Dictionary<string, DictTable>();
            foreach (var s in symbol)
            {
                var ds = await GetDSPiece(s, indi, tbegDate, tendDate, ext);
                dict[s] = ds;
            }
            return Ok(JsonConvert.SerializeObject(dict));
        }

        private async Task<DictTable> GetDSPiece(string symbol, string[] indis, DateTime tbeg, DateTime tend, string? ext)
        {
            var columns = GetSQLColumns(indis, tbeg, tend, ext);
            return await GetDSOne(symbol, columns, tbeg, tend);
        }

        private string GetSQLColumns(string[] indis, DateTime tbeg, DateTime tend, string? ext)
        {
            string sql="open_time, open";
            return sql;
        }

        private async Task<DictTable> GetDSOne(string symbol, string columns, DateTime tbeg, DateTime tend)
        {
            var sql = $"SELECT {columns} FROM spot_klines_1d " +
                $"WHERE symbol='{symbol}' AND" +
                $"      open_time >= '{tbeg}' and open_time <= '{tend}'" +
                $";";
            var db = new PgDB();
            var table = await db.QueryDataAsync(sql);
            return Convert(table);
        }
    }
}
