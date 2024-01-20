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

namespace BNLib.DB
{
    public partial class PgDB
    {
        private string _connString = "Host=localhost;Username=visitor;Password=123456;DataBase=bndata";
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
            _connString = $"Host={host};Port={port};Username={user};Password={passwd};DataBase={db}";
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

        public async Task InsertSpotTable(string symbol, List<BinanceSpotKline> data)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                foreach (var kline in data)
                {
                    var cmd = new NpgsqlCommand("INSERT INTO spot_klines_1d (" +
                                        "       symbol, open_time, open, high, low, close, volume," +
                                        "       close_time, quote_volume, trade_count, buy_volume, buy_quote_volume" +
                                        "       )" +
                                        "VALUES(@symbol, @open_time, @open, @high, @low, @close, @volume," +
                                        "       @close_time, @quote_volume, @trade_count, @buy_volume, @buy_quote_volume" +
                                        ");", conn);
                    cmd.Parameters.AddWithValue("symbol", symbol);
                    cmd.Parameters.AddWithValue("open_time", kline.OpenTime);
                    cmd.Parameters.AddWithValue("open", kline.OpenPrice);
                    cmd.Parameters.AddWithValue("high", kline.HighPrice);
                    cmd.Parameters.AddWithValue("low", kline.LowPrice);
                    cmd.Parameters.AddWithValue("close", kline.ClosePrice);
                    cmd.Parameters.AddWithValue("volume", kline.Volume);
                    cmd.Parameters.AddWithValue("close_time", kline.CloseTime);
                    cmd.Parameters.AddWithValue("quote_volume", kline.QuoteVolume);
                    cmd.Parameters.AddWithValue("trade_count", kline.TradeCount);
                    cmd.Parameters.AddWithValue("buy_volume", kline.TakerBuyBaseVolume);
                    cmd.Parameters.AddWithValue("buy_quote_volume", kline.TakerBuyQuoteVolume);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        // 空数据插入
        public async Task InsertSpotTableNullData(string symbol, List<BinanceSpotKline> data)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
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
                var cmd = new NpgsqlCommand("SELECT MIN(open_time), MAX(open_time) FROM spot_klines_1d " +
                "WHERE symbol = @symbol HAVING COUNT(*) > 0;", conn);
                cmd.Parameters.AddWithValue("symbol", sym);
                var reader = await cmd.ExecuteReaderAsync();
                DateTime begDate = DateTime.Today;
                DateTime endDate = begDate.AddDays(-1);
                while (reader.Read())
                {
                    begDate = reader.GetDateTime(0);
                    endDate = reader.GetDateTime(1);
                }
                return (begDate, endDate);
           }
        }
    }
}
