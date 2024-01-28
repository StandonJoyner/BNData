using Binance.Net.Enums;
using Binance.Net.Objects.Models;
using Binance.Net.Objects.Models.Spot;
using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using CryptoExchange.Net.CommonObjects;
using Microsoft.VisualBasic;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;

namespace BNLib.Frame
{
    public class KlinesUpdate
    {
        static ILogger _logger = Serilog.Log.ForContext<KlinesUpdate>();
        public KlinesUpdate() {
        }

        private static DateTime LastMonthEndDay(DateTime dt)
        {
            var result = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0);
            return result.AddMilliseconds(-1);
        }

        private async Task UpdateKlinesAll(PgDB db, MarketType market, string symbol, KlineInterval inv)
        { 
            var tbegDate = new DateTime(2017, 8, 17, 0, 0, 0);
            var tendDate = DateTime.UtcNow.AddDays(-1);
            if (inv != KlineInterval.OneDay)
                throw new Exception("Only support daily klines");
            var lines = await GetLackKlines(db, market, symbol,inv, tbegDate, tendDate);

            await db.InsertSpotTable(symbol, lines);

            if (inv == KlineInterval.OneDay)
                await FixDailyKlines(db, market, symbol, tbegDate, tendDate);
        }

        public async Task DownloadKlinesAll(PgDB db, MarketType market, string symbol, 
            KlineInterval inv, DateTime tbegDate, string outfile)
        {
            var tendDate = DateTime.UtcNow.AddDays(-1);
            if (inv != KlineInterval.OneDay)
                throw new Exception("Only support daily klines");
            try
            {
                var lines = await GetLackKlines(db, market, symbol, inv, tbegDate, tendDate);
                if (lines.Count == 0)
                    return;
                using (TextWriter output = new StreamWriter(outfile))
                {
                    // store lines to csv file
                    foreach (var line in lines)
                    {
                        DateTimeOffset dtoffset0 = new DateTimeOffset(line.OpenTime);
                        long opentm = dtoffset0.ToUnixTimeMilliseconds();

                        DateTimeOffset dtoffset1 = new DateTimeOffset(line.CloseTime);
                        long closetm = dtoffset1.ToUnixTimeMilliseconds();

                        output.WriteLine($"{opentm},{line.OpenPrice},{line.HighPrice},{line.LowPrice},{line.ClosePrice},{line.Volume},{closetm},{line.QuoteVolume},{line.TradeCount},{line.TakerBuyBaseVolume},{line.TakerBuyQuoteVolume},{line.Ignore}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"DownloadKlinesAll {symbol}");
            }
        }

        public async Task InsertKlinesAll(PgDB db, MarketType market, string symbol, KlineInterval inv, TextReader input)
        {
            if (inv != KlineInterval.OneDay)
                throw new Exception("Only support daily klines");
            List<BinanceSpotKline> lines = new List<BinanceSpotKline>();
            string? line = null;
            while ((line = input.ReadLine()) != null)
            {
                var items = line.Split(',');
                if (items.Length != 12)
                    throw new Exception("Invalid line");
                var kline = new BinanceSpotKline();
                kline.OpenTime = DateTime.Parse(items[0]);
                kline.OpenPrice = decimal.Parse(items[1]);
                kline.HighPrice = decimal.Parse(items[2]);
                kline.LowPrice = decimal.Parse(items[3]);
                kline.ClosePrice = decimal.Parse(items[4]);
                kline.Volume = decimal.Parse(items[5]);
                kline.CloseTime = DateTime.Parse(items[6]);
                kline.QuoteVolume = decimal.Parse(items[7]);
                kline.TradeCount = int.Parse(items[8]);
                kline.TakerBuyBaseVolume = decimal.Parse(items[9]);
                kline.TakerBuyQuoteVolume = decimal.Parse(items[10]);
                kline.Ignore = decimal.Parse(items[11]);
                lines.Add(kline);
            }

            await db.InsertSpotTable(symbol, lines);
        }

        public async Task<List<BinanceSpotKline>> GetLackKlinesAll(PgDB db, MarketType market, string symbol, 
            KlineInterval inv, DateTime tbegDate)
        {
            var lastDay = DateTime.UtcNow.AddDays(-1);
            return await GetLackKlines(db, market, symbol, inv, tbegDate, lastDay);
        }

        // [tbegDate, [curBeg, curEnd], tendDate]
        public async Task<List<BinanceSpotKline>> GetLackKlines(PgDB db, MarketType market, string symbol, 
            KlineInterval inv, DateTime tbegDate, DateTime tendDate)
        {
            var lines = new List<BinanceSpotKline>();
            (var begDate, var endDate ) = await db.GetSymbolCurDateRange(market, symbol);
            if (endDate < begDate)
            {
                lines = await GetKlines(market, symbol, inv, tbegDate, tendDate);
                return lines;
            }

            if (tbegDate < begDate && tendDate < begDate) // on left
            {
                var lines2 = await GetKlines(market, symbol, inv, tbegDate, begDate.AddMilliseconds(-1));
                lines.AddRange(lines2);
            }
            if (tbegDate > endDate && tendDate > endDate) // on right
            {
                var lines2 = await GetKlines(market, symbol, inv, endDate.AddMilliseconds(1), tendDate);
                lines.AddRange(lines2);
            }
            if (tbegDate >= begDate && tendDate <= endDate) // inside
                return lines;
            // overlapped
            if (tbegDate < begDate)
            {
                var lines2 = await GetKlines(market, symbol, inv, tbegDate, begDate.AddMilliseconds(-1));
                lines.AddRange(lines2);
            }

            if (endDate < tendDate)
            {
                var lines2 = await GetKlines(market, symbol, inv, endDate.AddMilliseconds(1), tendDate);
                lines.AddRange(lines2);
            }
            return lines;
        }

        public async Task<List<BinanceSpotKline>> GetKlines(MarketType market, string symbol, KlineInterval inv, DateTime tbegDate, DateTime tendDate)
        {
            var lines = new List<BinanceSpotKline>();
            if (tbegDate > tendDate)
                return lines;
            const int SMALL_DAY = 6;
            var diff = tendDate - tbegDate;
            if (diff.Days < SMALL_DAY)
            {
                var lines2 = await GetKlinesDaily(market, symbol, inv, tbegDate, tendDate);
                lines.AddRange(lines2);
                return lines;
            }

            // 开始日期所在月的最后一天
            var firstMonthEnd = LastMonthEndDay(tbegDate.AddMonths(1));
            var lastMonthBeg = new DateTime(tendDate.Year, tendDate.Month, 1);

            if (tbegDate.Day > (31 - SMALL_DAY))
            {
                var lines2 = await GetKlinesDaily(market, symbol, inv, tbegDate, firstMonthEnd);
                lines.AddRange(lines2);
            }
            else if (firstMonthEnd < tendDate) 
            {
                var lines2 = await GetKlinesMonthly(market, symbol, inv, tbegDate, firstMonthEnd);
                lines.AddRange(lines2);
            }
            else
            {
                var lines2 = await GetKlinesMonthly(market, symbol, inv, tbegDate, tendDate);
                lines.AddRange(lines2);
                return lines;
            }

            var monthBeg = firstMonthEnd.AddMilliseconds(1);
            var monthEnd = lastMonthBeg.AddMilliseconds(-1);
            if (monthEnd > monthBeg)
            {
                var lines2 = await GetKlinesMonthly(market, symbol, inv, monthBeg, monthEnd);
                lines.AddRange(lines2);
            }

            if (tendDate.Day < SMALL_DAY)
            {
                var lines2 = await GetKlinesDaily(market, symbol, inv, lastMonthBeg, tendDate);
                lines.AddRange(lines2);
            }
            else
            {
                var lines2 = await GetKlinesMonthly(market, symbol, inv, lastMonthBeg, tendDate);
                lines.AddRange(lines2);
            }
            return lines;
        }

        private async Task<List<BinanceSpotKline>> GetKlinesMonthly(MarketType market, string symbol, KlineInterval inv, DateTime beg, DateTime end)
        {
            if (beg > end)
                return new List<BinanceSpotKline>();
            _logger.Information($"GetKlineMonthly: {symbol} {inv} {beg} {end}");
            var now = DateTime.UtcNow;
            var mend = end;
            if (end > now)
                mend = now;
            if (end.Year == now.Year && end.Month == now.Month)
                mend = LastMonthEndDay(end);

            // update monthly klines frome beg to last month end
            var lines = await KLineDownload.DownloadMonthlyKLines(market, symbol, inv, beg, mend);
            lines = lines.Where(line => line.OpenTime >= beg && line.OpenTime <= mend).ToList();
            
            // update current month klines
            if (mend < end)
            {
                var dbeg = mend.AddMilliseconds(1);
                if (dbeg < beg)
                    dbeg = beg;
                var lines2 = await GetKlinesDaily(market, symbol, inv, dbeg, end);
                lines.AddRange(lines2);
            }
            return lines;
        }

        private async Task<List<BinanceSpotKline>> GetKlinesDaily(MarketType market, string symbol, KlineInterval inv, DateTime beg, DateTime end)
        {
            if (beg > end)
                return new List<BinanceSpotKline>();
            _logger.Information($"GetKlineDaily: {symbol} {inv} {beg} {end}");
            if (end > DateTime.Now)
                end = DateTime.Now;
            var lines = await KLineDownload.DownloadDailyKLines(market, symbol, inv, beg, end);
            lines = lines.Where(line => line.OpenTime >= beg && line.OpenTime <= end).ToList();
            return lines;
        }
        // 数据校准，自动补齐缺失的K线
        public async Task FixDailyKlines(PgDB db, MarketType market, string symbol, DateTime tbegDate, DateTime tendDate)
        {
            List<BinanceSpotKline> dblines;
            try
            {
                dblines = await db.QueryKlinesAsync(symbol, tbegDate, tendDate);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"FixDailyKlines {symbol}");
                return;
            }

            var lines = new List<BinanceSpotKline>();
            var begDate = tbegDate;
            // 去掉开始日期前的数据
            if (dblines.Count > 0 && dblines[0].OpenTime > tbegDate)
                begDate = dblines[0].OpenTime;
            foreach (var line in dblines)
            {
                while (line.OpenTime > begDate)
                {
                    var one = new BinanceSpotKline();
                    one.OpenTime = begDate;
                    lines.Add(line);
                    begDate = begDate.AddDays(1);
                }
                begDate = line.OpenTime.AddDays(1);
            }
            while (begDate <= tendDate)
            {
                var one = new BinanceSpotKline();
                one.OpenTime = begDate;
                lines.Add(one);
                begDate = begDate.AddDays(1);
            }
            foreach (var line in lines)
            {
                _logger.Information($"缺失 {symbol}, {line.OpenTime}");
            }
            //if (lines.Count > 0)
            //    await db.InsertSpotTableNullData(symbol, lines);
        }
    }
}
