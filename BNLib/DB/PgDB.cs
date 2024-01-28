using BNLib.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using Binance.Net.Objects.Models.Spot;
using Microsoft.VisualBasic;
using System.Reflection;
using System.Data;
using Npgsql.Bulk;
using Microsoft.EntityFrameworkCore;
using CryptoExchange.Net.CommonObjects;
using Serilog;

namespace BNLib.DB
{
    public partial class PgDB
    {
        private string _connString = "Host=localhost;Username=visitor;Password=123456;DataBase=bndata";
        static ILogger _logger = Serilog.Log.ForContext<PgDB>();
        public PgDB()
        {
        }
        ~PgDB()
        {
        }

        public async Task InitDB(string host, string port, string user, string passwd, string db)
        {
            Connect(host, port, user, passwd, db);

            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();

                var assembly = Assembly.GetExecutingAssembly();
                var resourceName = assembly.GetManifestResourceNames()
                    .Single(str => str.EndsWith("Resources.init.sql"));
                using (Stream? stream = assembly.GetManifestResourceStream(resourceName))
                {
                    if (stream == null)
                        throw new Exception("Cannot find init.sql");
                    StreamReader reader = new StreamReader(stream);
                    string result = reader.ReadToEnd();

                    var cmd = new NpgsqlCommand(result, conn);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public void Connect(string host, string port, string user, string passwd, string db)
        {
            _connString = $"Host={host};Port={port};Username={user};Password={passwd};DataBase={db};" +
                $"CommandTimeout=300";
        }

        public async Task<DataTable> QueryDataAsync(string sql)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();
                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        var dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }

        public async Task<bool> ExecuteSQLAsync(string sql)
        {
            try
            {
                using (var conn = new NpgsqlConnection(_connString))
                {
                    await conn.OpenAsync();
                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"ExecuteSQLAsync {sql}");
                return false;
            }
        }

        public async Task<List<BinanceSpotKline>> QueryKlinesAsync(string symbol, DateTime beg, DateTime end)
        {
            string begStr = beg.ToString("yyyy-MM-dd");
            string endStr = end.ToString("yyyy-MM-dd");
            var sql = $"SELECT * from spot_klines_1d " +
                $"WHERE symbol = '{symbol}' AND open_time >= '{begStr}' AND open_time <= '{endStr}'" +
                $"ORDER BY open_time ASC;";
            var dataTable = await QueryDataAsync(sql);
            var lines = new List<BinanceSpotKline>();
            foreach (DataRow row in dataTable.Rows)
            {
                var line = new BinanceSpotKline();
                line.OpenTime = row.Field<DateTime>("open_time");
                line.OpenPrice = row.Field<decimal>("open");
                line.HighPrice = row.Field<decimal>("high");
                line.LowPrice = row.Field<decimal>("low");
                line.ClosePrice = row.Field<decimal>("close");
                line.Volume = row.Field<decimal>("volume");
                line.CloseTime = row.Field<DateTime>("close_time");
                line.QuoteVolume = row.Field<decimal>("quote_volume");
                line.TradeCount = row.Field<int>("trade_count");
                line.TakerBuyBaseVolume = row.Field<decimal>("buy_volume");
                line.TakerBuyQuoteVolume = row.Field<decimal>("buy_quote_volume");
                lines.Add(line);
            }
            return lines;
        }

        class spot_klines_1d
        {
            public string symbol { get; set; }
            public DateTime date { get; set; }
            public decimal open { get; set; }
            public decimal high { get; set; }
            public decimal low { get; set; }
            public decimal close { get; set; }
            public decimal volume { get; set; }
            public DateTime close_time { get; set; }
            public decimal quote_volume { get; set; }
            public int trade_count { get; set; }
            public decimal buy_volume { get; set; }
            public decimal buy_quote_volume { get; set; }
        }

        public async Task<bool> InsertSpotTable(string symbol, List<BinanceSpotKline> data)
        {
            try
            {
                using (var conn = new NpgsqlConnection(_connString))
                {
                    await conn.OpenAsync();

                    using (var writer = conn.BeginBinaryImport("COPY spot_klines_1d (" +
                                        "       symbol, open_time, open, high, low, close, volume," +
                                        "       close_time, quote_volume, trade_count, buy_volume, buy_quote_volume" +
                        ") FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var item in data)
                        {
                            writer.StartRow();
                            writer.Write(symbol);
                            writer.Write(item.OpenTime);
                            writer.Write(item.OpenPrice);
                            writer.Write(item.HighPrice);
                            writer.Write(item.LowPrice);
                            writer.Write(item.ClosePrice);
                            writer.Write(item.Volume);
                            writer.Write(item.CloseTime);
                            writer.Write(item.QuoteVolume);
                            writer.Write(item.TradeCount);
                            writer.Write(item.TakerBuyBaseVolume);
                            writer.Write(item.TakerBuyQuoteVolume);
                        }
                        writer.Complete();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"InsertSpotTable {symbol} {data.Count}");
                return false;
            }
        }

        // 空数据插入
        public async Task InsertSpotTableNullData(string symbol, List<BinanceSpotKline> data)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();
                foreach (var kline in data)
                {
                    var cmd = new NpgsqlCommand("INSERT INTO spot_klines_1d" +
                        "       (symbol, open_time)" +
                        "VALUES(@symbol, @open_time)"
                        );
                    cmd.Parameters.AddWithValue("symbol", symbol);
                    cmd.Parameters.AddWithValue("open_time", kline.OpenTime);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<(DateTime, DateTime)> GetSymbolCurDateRange(MarketType marketType, string sym)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();

                var cmd = new NpgsqlCommand("SELECT MIN(open_time), MAX(open_time) FROM spot_klines_1d " +
"WHERE symbol = @symbol HAVING COUNT(*) > 0;", conn);
                cmd.Parameters.AddWithValue("symbol", sym);

                var reader = await cmd.ExecuteReaderAsync();
                DateTime begDate = DateTime.UtcNow;
                DateTime endDate = begDate.AddDays(-1);
                while (reader.Read())
                {
                    begDate = reader.GetDateTime(0);
                    endDate = reader.GetDateTime(1);
                }
                DateTime.SpecifyKind(begDate, DateTimeKind.Utc);
                DateTime.SpecifyKind(endDate, DateTimeKind.Utc);
                return (begDate, endDate);
            }
        }
    }
}
