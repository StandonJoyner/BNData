using Binance.Net.Objects.Models.Spot;
using CryptoExchange.Net.Attributes;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BNLib.BN
{
    public class Utils
    {
        public static async Task<byte[]> DownloadZip(string fullurl)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    return await client.GetByteArrayAsync(fullurl);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return null;
        }

        public static async Task<string[]> ParseZip(byte[] data)
        {
            if (data == null)
                return null;
            var ms = new System.IO.MemoryStream(data);
            var zip = new System.IO.Compression.ZipArchive(ms);
            foreach (var entry in zip.Entries)
            {
                if (entry.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                {
                    var sr = new System.IO.StreamReader(entry.Open());
                    var lines = await sr.ReadToEndAsync();
                    return lines.Split('\n');
                }
            }
            return null;
        }

        public static List<BinanceSpotKline> ParseKlinesCSV(string[] lines)
        {
            var table = new List<BinanceSpotKline>();
            foreach (var line in lines)
            {
                if (string.IsNullOrEmpty(line))
                    continue;
                var items = line.Split(',');
                var data = new BinanceSpotKline();
                DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(items[0]));
                data.OpenTime = dateTimeOffset.DateTime;
                data.OpenPrice = decimal.Parse(items[1]);
                data.HighPrice = decimal.Parse(items[2]);
                data.LowPrice = decimal.Parse(items[3]);
                data.ClosePrice = decimal.Parse(items[4]);
                data.Volume = decimal.Parse(items[5]);
                dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(items[6]));
                data.CloseTime = dateTimeOffset.DateTime;
                data.QuoteVolume = decimal.Parse(items[7]);
                data.TradeCount = int.Parse(items[8]);
                data.TakerBuyBaseVolume = decimal.Parse(items[9]);
                data.TakerBuyQuoteVolume = decimal.Parse(items[10]);
                table.Add(data);
            }
            return table;
        }

        public static string MapEnum(Enum value)
        {
            var type = value.GetType();
            var name = Enum.GetName(type, value);
            if (name != null)
            {
                var field = type.GetField(name);
                if (field != null)
                {
                    if (Attribute.GetCustomAttribute(field, typeof(MapAttribute)) is MapAttribute attr)
                    {
                        return attr.Values[0];
                    }
                }
            }
            return "";
        }
    }
}
