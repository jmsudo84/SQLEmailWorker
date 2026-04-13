using Microsoft.Extensions.Hosting.WindowsServices;
using SqlEmailWorker;
using System.IO;

// STAP 1: Forceer de werkmap naar de locatie van de .exe
// Dit is de oplossing voor Error 1053 (het vinden van appsettings.json)
Directory.SetCurrentDirectory(AppDomain.CurrentDomain.BaseDirectory);

var builder = Host.CreateApplicationBuilder(args);

// STAP 2: Configureer de Windows Service eigenschappen
builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "SqlClearingReporter";
});

// STAP 3: Voeg je Worker toe
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

// STAP 4: Run de host
host.Run();