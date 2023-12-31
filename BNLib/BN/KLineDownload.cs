using BNLib.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Binance.Net.Clients;
using Binance.Net.Objects.Models.Spot;
using Binance.Net.Enums;
using Binance.Net.Objects.Models;

namespace BNLib.BN
{
    public class KLineDownload
    {
        public static string GetMonthlyKlinesUrl(MarketType type, string code, KlineInterval inv, DateTime dt)
        {
            string interval;
            if (inv == KlineInterval.OneMonth)
                interval = "1mo";
            else
                interval = Utils.MapEnum(inv);
            var fname = $"{code.ToUpper()}-{interval}-{dt.Year}-{dt.Month.ToString("D2")}.zip";
            if (type == MarketType.SPOT)
                return $"https://data.binance.vision/data/spot/monthly/klines/{code.ToUpper()}/{interval}/{fname}";
            else
                return "";
        }

        public static string GetDailyKlinesUrl(MarketType type, string code, KlineInterval inv, DateTime dt)
        {
            string interval;
            if (inv == KlineInterval.OneMonth)
                interval = "1mo";
            else
                interval = Utils.MapEnum(inv);
            var fname = $"{code.ToUpper()}-{interval}-{dt.Year}-{dt.Month.ToString("D2")}-{dt.Day.ToString("D2")}.zip";
            if (type == MarketType.SPOT)
                return $"https://data.binance.vision/data/spot/daily/klines/{code.ToUpper()}/{interval}/{fname}";
            else
                return "";
        }

        // not include month of dt2
        public static async Task<List<BinanceSpotKline>> DownloadMonthlyKLines(MarketType type, string symbol, KlineInterval inv, DateTime dt1, DateTime dt2)
        {
            var klines = new List<BinanceSpotKline>();
            if (dt1 > dt2)
                return klines;
            dt1 = new DateTime(dt1.Year, dt1.Month, 1);
            while (dt1 <= dt2)
            {
                var url = GetMonthlyKlinesUrl(type, symbol, inv, dt1);
                var zipData = await Utils.DownloadZip(url);
                if (zipData != null)
                {
                    var lines = await Utils.ParseZip(zipData);
                    var table = Utils.ParseKlinesCSV(lines);
                    klines.AddRange(table);
                }
                else 
                {
                    Console.WriteLine($"Download {url} failed.");
                }
                dt1 = dt1.AddMonths(1);
            }
            return klines;
        }

        public static async Task<List<BinanceSpotKline>> DownloadDailyKLines(MarketType type, string symbol, KlineInterval inv, DateTime dt1, DateTime dt2)
        {
            var klines = new List<BinanceSpotKline>();
            if (dt1 > dt2)
                return klines;
            dt1 = new DateTime(dt1.Year, dt1.Month, dt1.Day);
            while (dt1 <= dt2)
            {
                var url = GetDailyKlinesUrl(type, symbol, inv, dt1);
                var zipData = await Utils.DownloadZip(url);
                if (zipData != null)
                {
                    var lines = await Utils.ParseZip(zipData);
                    var table = Utils.ParseKlinesCSV(lines);
                    klines.AddRange(table);
                }
                else
                {
                    Console.WriteLine($"Download {url} failed.");
                }
                dt1 = dt1.AddDays(1);
            }
            return klines;
        }
    }
}
