using Binance.Net.Clients;
using Binance.Net.Enums;
using Binance.Net.Objects.Models.Spot;
using BNLib.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using BNLib.Frame;

namespace BNLib.BN
{
    public class ExchangeInfo
    {
        private static BinanceExchangeInfo[] _cache = new BinanceExchangeInfo[3];
        private static Dictionary<string, BinanceSymbol> _symbolSpotCache = new Dictionary<string, BinanceSymbol>();
        private static AsyncLock _spotLock = new AsyncLock();

        public static async Task<BinanceExchangeInfo> GetExchangeInfo(MarketType type)
        {
            if (type == MarketType.SPOT)
                return await GetSpotExchangeInfo();
            else
                return null;
        }

        public static async Task<BinanceExchangeInfo> GetSpotExchangeInfo()
        {
            using (await _spotLock.LockAsync())
            {
                if (_cache[0] != null)
                    return _cache[0];
                var client = new BinanceRestClient();
                var info = await client.SpotApi.ExchangeData.GetExchangeInfoAsync();
                _cache[0] = info.Data;
                return info.Data;
            }
        }

        public static async Task<IEnumerable<string>> GetAllSymbols(MarketType type, bool tradingOnly)
        {
            var info = await GetExchangeInfo(type);
            if (tradingOnly)
                return info.Symbols.Where(s => s.Status == SymbolStatus.Trading).Select(s => s.Name);
            else
                return info.Symbols.Select(s => s.Name);
        }

        public static async Task<BinanceSymbol> GetSymbol(MarketType type, string symbol)
        {
            if (type == MarketType.SPOT)
            {
                return await GetSpotSymbols(symbol);
            }
            else
            {
                var info = await GetExchangeInfo(type);
                return info.Symbols.FirstOrDefault(s => s.Name == symbol);
            }
        }

        public static async Task<BinanceSymbol> GetSpotSymbols(string symbol)
        {
            lock (_symbolSpotCache)
            {
                if (_symbolSpotCache.ContainsKey(symbol))
                    return _symbolSpotCache[symbol];
            }
            var info = await GetExchangeInfo(MarketType.SPOT);
            var s = info.Symbols.FirstOrDefault(s => s.Name == symbol);
            lock (_symbolSpotCache)
            {
                _symbolSpotCache[symbol] = s;
            }
            return s;
        }
    }
}
