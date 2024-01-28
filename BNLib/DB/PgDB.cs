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
using static System.Net.Mime.MediaTypeNames;
using static System.Runtime.InteropServices.JavaScript.JSType;

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

        public async Task<DataTable> QueryDataHistory(string symbol, string columns, DateTime tbeg, DateTime tend)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();

                var cmd = new NpgsqlCommand("SELECT " + columns + " FROM spot_klines_1d " +
                    "WHERE symbol=@symbol AND open_time >= @tbeg AND open_time <= @tend " +
                                           "ORDER BY open_time ASC;", conn);
                cmd.Parameters.AddWithValue("symbol", symbol);
                cmd.Parameters.AddWithValue("tbeg", tbeg);
                cmd.Parameters.AddWithValue("tend", tend);
                cmd.ExecuteNonQuery();

                var reader = await cmd.ExecuteReaderAsync();
                DataTable dt = new DataTable();
                dt.Load(reader);
                return dt;
            }
        }

        public async Task<List<BinanceSpotKline>> QueryKlinesAsync(string symbol, DateTime beg, DateTime end)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                await conn.OpenAsync();

                var cmd = new NpgsqlCommand("SELECT * FROM spot_klines_1d " +
                                           "WHERE symbol=@symbol AND open_time >= @tbeg AND open_time <= @tend " +
                                                                                         "ORDER BY open_time ASC;", conn);
                cmd.Parameters.AddWithValue("symbol", symbol);
                cmd.Parameters.AddWithValue("tbeg", beg);
                cmd.Parameters.AddWithValue("tend", end);
                cmd.ExecuteNonQuery();

                var reader = await cmd.ExecuteReaderAsync();
                DataTable dt = new DataTable();
                dt.Load(reader);

                var lines = new List<BinanceSpotKline>();
                foreach (DataRow row in dt.Rows)
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

        public async Task UpdateSymbolInfo(BinanceSymbol symbol)
        {
            try
            {
                using (var conn = new NpgsqlConnection(_connString))
                {
                    await conn.OpenAsync();

                    var cmd = new NpgsqlCommand(@"
                INSERT INTO spot_symbols_info(
                    symbol, quote_asset, base_asset, 
                    quote_asset_precision, base_asset_precision, 
                    quote_fee_precision, base_fee_precision,
                    iceberg_allowed, oco_allowed, 
                    quote_order_qty_market_allowed, 
                    is_spot_trading_allowed, is_margin_trading_allowed, 
                    created_at, updated_at)
                VALUES (
                    @symbol, @quote_asset, @base_asset, 
                    @quote_asset_precision, @base_asset_precision, 
                    @quote_fee_precision, @base_fee_precision,
                    @iceberg_allowed, @oco_allowed, 
                    @quote_order_qty_market_allowed, 
                    @is_spot_trading_allowed, @is_margin_trading_allowed, 
                    @created_at, @updated_at)
                ON CONFLICT (symbol) DO UPDATE SET
                    quote_asset = EXCLUDED.quote_asset,
                    base_asset = EXCLUDED.base_asset,
                    quote_asset_precision = EXCLUDED.quote_asset_precision,
                    base_asset_precision = EXCLUDED.base_asset_precision,
                    quote_fee_precision = EXCLUDED.quote_fee_precision,
                    base_fee_precision = EXCLUDED.base_fee_precision,
                    iceberg_allowed = EXCLUDED.iceberg_allowed,
                    oco_allowed = EXCLUDED.oco_allowed,
                    quote_order_qty_market_allowed = EXCLUDED.quote_order_qty_market_allowed,
                    is_spot_trading_allowed = EXCLUDED.is_spot_trading_allowed,
                    is_margin_trading_allowed = EXCLUDED.is_margin_trading_allowed,
                    updated_at = EXCLUDED.updated_at
                ", conn);

                    string pair = symbol.BaseAsset + "/" + symbol.QuoteAsset;
                    // 添加参数
                    cmd.Parameters.AddWithValue("symbol", pair);
                    cmd.Parameters.AddWithValue("quote_asset", symbol.QuoteAsset);
                    cmd.Parameters.AddWithValue("base_asset", symbol.BaseAsset);
                    cmd.Parameters.AddWithValue("quote_asset_precision", symbol.QuoteAssetPrecision);
                    cmd.Parameters.AddWithValue("base_asset_precision", symbol.BaseAssetPrecision);
                    cmd.Parameters.AddWithValue("quote_fee_precision", symbol.QuoteFeePrecision);
                    cmd.Parameters.AddWithValue("base_fee_precision", symbol.BaseFeePrecision);

                    cmd.Parameters.AddWithValue("iceberg_allowed", symbol.IceBergAllowed);
                    cmd.Parameters.AddWithValue("oco_allowed", symbol.OCOAllowed);
                    cmd.Parameters.AddWithValue("quote_order_qty_market_allowed", symbol.QuoteOrderQuantityMarketAllowed);
                    cmd.Parameters.AddWithValue("is_spot_trading_allowed", symbol.IsSpotTradingAllowed);
                    cmd.Parameters.AddWithValue("is_margin_trading_allowed", symbol.IsMarginTradingAllowed);
                    cmd.Parameters.AddWithValue("created_at", DateTime.UtcNow);
                    cmd.Parameters.AddWithValue("updated_at", DateTime.UtcNow);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"UpdateSymbolInfo {symbol.Name}");
            }
        }

        public async Task RecordLog(string ip, string content, int cells, string err)
        {
            try
            {
                using (var conn = new NpgsqlConnection(_connString))
                {
                    await conn.OpenAsync();

                    var cmd = new NpgsqlCommand("INSERT INTO request_logs (ip, sql, cells, err) " +
                        "VALUES (@ip, @sql, @cells, @err);", conn);
                    cmd.Parameters.AddWithValue("ip", ip);
                    cmd.Parameters.AddWithValue("sql", content);
                    cmd.Parameters.AddWithValue("cells", cells);
                    cmd.Parameters.AddWithValue("err", err);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"RecordLog {ip} {content} {cells} {err}");
            }
        }
    }
}
