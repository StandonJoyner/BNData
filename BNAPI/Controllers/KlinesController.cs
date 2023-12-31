using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data;
using BNLib.DB;
using Newtonsoft.Json;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace BNAPI.Controllers
{
    [Route("api/BN/[controller]")]
    [ApiController]
    public class KlinesController : ControllerBase
    {
        // POST api/BN/<KlinesController>
        [HttpPost("query")]
        public async Task<IActionResult> Query([FromBody] string sql)
        {
            try
            {
                DataTable dt = await new PgDB().QueryDataAsync(sql);

                var dict = new Dictionary<string, List<object>>();
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
                var settings = new JsonSerializerSettings
                {
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                    Formatting = Formatting.None
                };
                var json = JsonConvert.SerializeObject(dict, settings);
                return Ok(json);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}
