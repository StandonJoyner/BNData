namespace BNAPI.Common
{
    public class DBConfig
    {
        static DBConfig? _instance;
        public static DBConfig Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new DBConfig();
                return _instance;
            }
        }
        public DBConfig()
        {
            // appsetings
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            var config = builder.Build();
            config.GetSection("Databasse").Bind(this);
        }
        public string Host { get; set; } = "localhost";
        public string Port { get; set; } = "5432";
        public string User { get; set; } = "visitor";
        public string Password { get; set; } = "123456";
        public string DB { get; set; } = "bndata";
    }
}
