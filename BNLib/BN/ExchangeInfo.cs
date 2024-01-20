using Binance.Net.Clients;
using Binance.Net.Enums;
using Binance.Net.Objects.Models.Spot;
using BNLib.Enums;
using BNLib.Frame;
using System.Runtime.Caching;

namespace BNLib.BN
{
    public class ExchangeInfo
    {
        private static BinanceExchangeInfo[] _cache = new BinanceExchangeInfo[3];
        private static ObjectCache _symbolSpotCache = MemoryCache.Default;
        private static AsyncLock _spotLock = new AsyncLock();
        private static CacheItemPolicy _cachePolicy = new CacheItemPolicy { AbsoluteExpiration = DateTime.Today.AddDays(1) };

        public static async Task<BinanceExchangeInfo?> GetExchangeInfo(MarketType type)
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
            if (info == null)
            {
                return new List<string>();
            }
            if (tradingOnly)
                return info.Symbols.Where(s => s.Status == SymbolStatus.Trading).Select(s => s.Name);
            else
                return info.Symbols.Select(s => s.Name);
        }

        public static async Task<BinanceSymbol?> GetSymbol(MarketType type, string symbol)
        {
            if (type == MarketType.SPOT)
            {
                return await GetSpotSymbols(symbol);
            }
            else
            {
                var info = await GetExchangeInfo(type);
                if (info == null)
                    return null;
                var s = info.Symbols.FirstOrDefault(s => s.Name == symbol);
                return s;
            }
        }

        public static async Task<BinanceSymbol?> GetSpotSymbols(string symbol)
        {
            if (_symbolSpotCache.Contains(symbol))
                return (BinanceSymbol)_symbolSpotCache.Get(symbol);

            var info = await GetExchangeInfo(MarketType.SPOT);
            if (info == null)
                return null;
            var s = info.Symbols.FirstOrDefault(s => s.Name == symbol);
            if (s == null)
                return new BinanceSymbol();
            _symbolSpotCache.Add(symbol, s, _cachePolicy);
            return s;
        }
    }
}
