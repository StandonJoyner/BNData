using Serilog;
using Serilog.Events;
using Microsoft.AspNetCore.ResponseCompression;
using System.IO.Compression;
using System.Runtime.InteropServices;

var builder = WebApplication.CreateBuilder(args);
InitLog(builder);
// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
// Add Gzip Compression
builder.Services.Configure<GzipCompressionProviderOptions>(options =>
{
    options.Level = CompressionLevel.Fastest;
});
builder.Services.AddResponseCompression(options =>
{
    options.Providers.Add<GzipCompressionProvider>();
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

void InitLog(WebApplicationBuilder builder)
{
    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
        .Enrich.FromLogContext()
        .WriteTo.File("logs/log-.log", rollingInterval: RollingInterval.Day)
        .WriteTo.Console()
        .CreateBootstrapLogger();
    Log.Information("-------------{value}-------------", "System Info Begin");
    Log.Information("Platform: {value}", (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
                               RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "OSX" :
                                                      RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" : "Unknown"));
    Log.Information("Architecture: {value}", RuntimeInformation.OSArchitecture);
    Log.Information("Description: {value}", RuntimeInformation.OSDescription);
    Log.Information("ProcessArchitecture: {value}", RuntimeInformation.ProcessArchitecture);
    Log.Information("X64: {value}", (Environment.Is64BitOperatingSystem ? "Yes" : "No"));
    Log.Information("CPU CORE: {value}", Environment.ProcessorCount);
    Log.Information("HostName: {value}", Environment.MachineName);
    Log.Information("Version: {value}", Environment.OSVersion);
    Log.Information("-------------{value}-------------", "System Info End");

    builder.Host.UseSerilog((context, services, configuration) => configuration
                    .ReadFrom.Configuration(context.Configuration)
                    .ReadFrom.Services(services)
                    .Enrich.FromLogContext()
                    .WriteTo.File("logs/log-.log", rollingInterval: RollingInterval.Day)
                    .WriteTo.Console()
                    );
}
