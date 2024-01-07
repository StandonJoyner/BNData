using Binance.Net.Enums;
using Binance.Net.Objects.Models.Spot;
using BNLib.BN;
using BNLib.DB;
using BNLib.Enums;
using CryptoExchange.Net.CommonObjects;
using Microsoft.VisualBasic;
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
        public KlinesUpdate() { }

        private static DateTime LastMonthEndDay(DateTime dt)
        {
            var result = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0);
            return result.AddMilliseconds(-1);
        }

        public async Task UpdateKlinesAll(MarketType market, string symbol, KlineInterval inv)
        { 
            var tbegDate = new DateTime(2017, 8, 17, 0, 0, 0);
            var tendDate = DateTime.Today.AddDays(-1);
            if (inv != KlineInterval.OneDay)
                throw new Exception("Only support daily klines");
            var lines = await GetLackKlines(market, symbol,inv, tbegDate, tendDate);
            var db = new PgDB();
            db.Connect("postgres", "123456", "bndata");
            await db.InsertSpotTable(symbol, lines);
            if (inv == KlineInterval.OneDay)
                await FixDailyKlines(market, symbol, tbegDate, tendDate);
        }

        public async Task<List<BinanceSpotKline>> GetLackKlinesAll(MarketType market, string symbol, KlineInterval inv)
        {
            var tbegDate = new DateTime(2017, 8, 17, 0, 0, 0);
            return await GetLackKlines(market, symbol, inv, tbegDate, DateTime.Now);
        }

        // [tbegDate, [curBeg, curEnd], tendDate]
        public async Task<List<BinanceSpotKline>> GetLackKlines(MarketType market, string symbol, KlineInterval inv, DateTime tbegDate, DateTime tendDate)
        {
            var lines = new List<BinanceSpotKline>();
            var db = new PgDB();
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
            Console.WriteLine($"GetKlineMonthly: {symbol} {inv} {beg} {end}");
            var now = DateTime.Now;
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
            Console.WriteLine($"GetKlineDaily: {symbol} {inv} {beg} {end}");
            if (end > DateTime.Now)
                end = DateTime.Now;
            var lines = await KLineDownload.DownloadDailyKLines(market, symbol, inv, beg, end);
            lines = lines.Where(line => line.OpenTime >= beg && line.OpenTime <= end).ToList();
            return lines;
        }
        // 数据校准，自动补齐缺失的K线
        public async Task FixDailyKlines(MarketType market, string symbol, DateTime tbegDate, DateTime tendDate)
        {
            var db = new PgDB();
            db.Connect("postgres", "123456", "bndata");
            var dblines = await db.QueryKlinesAsync(symbol, tbegDate, tendDate);
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
                Console.WriteLine($"缺失 {symbol}, {line.OpenTime}");
            }
            //if (lines.Count > 0)
            //    await db.InsertSpotTableNullData(symbol, lines);
        }
    }
}
