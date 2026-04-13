using System.Text;
using Dapper;
using MailKit.Net.Smtp;
using Microsoft.Data.SqlClient;
using MimeKit;
using System.Globalization;

namespace SqlEmailWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private DateTime _lastRunDate = DateTime.MinValue;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    var settings = _configuration.GetSection("EmailSettings");
    bool runOnStartup = bool.Parse(settings["RunOnStartup"] ?? "false");
    string scheduledTime = settings["ScheduleTime"] ?? "08:30";

    _logger.LogInformation("Worker gestart. Geplande tijd: {time}", scheduledTime);

    // Bij het opstarten checken of we direct een run moeten doen
    if (runOnStartup)
    {
        _logger.LogInformation("Test-run: Directe uitvoering bij opstarten...");
        // We geven de stoppingToken mee voor het geval de service direct weer wordt gestopt
        await HaalDataEnVerstuurEmail(stoppingToken);
        _lastRunDate = DateTime.Now; 
    }

    // De hoofdlus die blijft draaien zolang de service niet gestopt wordt
    while (!stoppingToken.IsCancellationRequested)
    {
        var nu = DateTime.Now;
        var tijdNu = nu.ToString("HH:mm");

        // Controleer of de huidige tijd overeenkomt met de geplande tijd 
        // EN of we vandaag nog niet hebben gedraaid
        if (tijdNu == scheduledTime && _lastRunDate.Date != nu.Date)
        {
            _logger.LogInformation("Starten van geplande dagelijkse rapportage...");
            
            // Ook hier geven we de stoppingToken door naar de SQL- en Mail-logica
            await HaalDataEnVerstuurEmail(stoppingToken);
            
            _lastRunDate = nu;
            _logger.LogInformation("Rapportage voltooid. Volgende run gepland voor morgen om {time}.", scheduledTime);
        }

        // Wacht 30 seconden voor de volgende check. 
        // De Task.Delay luistert naar de stoppingToken om direct te onderbreken bij stop.
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Dit gebeurt wanneer de service wordt gestopt tijdens de delay; dit is normaal gedrag.
            break;
        }
    }

    _logger.LogInformation("Worker wordt afgesloten...");
}
    private async Task HaalDataEnVerstuurEmail(CancellationToken ct = default)
{
    try
    {
        var settings = _configuration.GetSection("EmailSettings");
        int dayOffset = int.Parse(settings["DayOffset"] ?? "0");
        
        var connString = _configuration.GetConnectionString("DefaultConnection");

        // 'using' zorgt ervoor dat de SQL-verbinding direct wordt gesloten en 
        // vrijgegeven aan de pool, zelfs als er een fout optreedt.
        using (var connection = new SqlConnection(connString))
        {
            var query = @"
                SELECT 
                    clearingDate, 
                    MerchantName, 
                    MerchantCode, 
                    amount, 
                    CASE state 
                        WHEN 0 THEN 'Created' 
                        WHEN 1 THEN 'Pending' 
                        WHEN 2 THEN 'Succesfull' 
                        WHEN 3 THEN 'Failed' 
                        ELSE 'Unknown' 
                    END AS StateText
                FROM Clearings 
                WHERE CAST(clearingDate AS DATE) = CAST(DATEADD(day, @Offset, GETDATE()) AS DATE)
                ORDER BY clearingDate DESC";

            // We geven de CancellationToken (ct) door aan Dapper. 
            // Als de service stopt tijdens een lange query, wordt deze netjes afgebroken.
            var data = await connection.QueryAsync(new CommandDefinition(query, new { Offset = dayOffset }, cancellationToken: ct));

            var emailBody = BouwHtmlTabel(data, dayOffset);
            
            // Ook hier geven we de token door aan de e-mail functie
            await VerstuurEmail(emailBody, dayOffset, ct);
            
            _logger.LogInformation("E-mail succesvol verzonden voor offset {Offset}.", dayOffset);
        }
    }
    catch (OperationCanceledException)
    {
        _logger.LogWarning("De SQL-taak is geannuleerd omdat de service werd gestopt.");
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Fout tijdens uitvoeren van SQL taak of verzenden e-mail.");
    }
}
    private string BouwHtmlTabel(IEnumerable<dynamic> data, int offset)
{
    // Maak een Nederlandse cultuur aan voor de opmaak
    var nlCulture = new CultureInfo("nl-NL");
    var rapportDatum = DateTime.Now.AddDays(offset);
    var sb = new StringBuilder();
    
    sb.Append("<h2 style='font-family: Arial, sans-serif; color: #333;'>Dagelijks Clearing Overzicht</h2>");
    sb.Append($"<p style='font-family: Arial, sans-serif;'>Rapport voor datum: {rapportDatum:dd-MM-yyyy}</p>");

    if (!data.Any())
    {
        sb.Append("<table border='0' cellpadding='8' style='border-collapse: collapse; font-family: Arial, sans-serif; width: 100%;'>");
        sb.Append("<tr><td style='text-align: center; padding: 40px; color: red; font-size: 20px; font-weight: bold;'>");
        sb.Append("GEEN CLEARINGS VOOR VANDAAG");
        sb.Append("</td></tr></table>");
        return sb.ToString();
    }

    sb.Append("<table border='0' cellpadding='8' style='border-collapse: collapse; font-family: Arial, sans-serif; width: 100%; min-width: 600px;'>");
    sb.Append("<tr style='background-color: #004a99; color: white; text-align: left;'>");
    sb.Append("<th>Datum/Tijd</th><th>Merchant</th><th>Code</th><th>Bedrag</th><th>Status</th></tr>");

    decimal totaal = 0;
    foreach (var item in data)
    {
        decimal bedrag = (decimal)(item.amount ?? 0m);
        totaal += bedrag;
        
        string statusKleur = item.StateText == "Succesfull" ? "#28a745" : (item.StateText == "Failed" ? "#dc3545" : "#333");

        // Gebruik nlCulture voor de opmaak (geeft 1.250,50)
        string geformatteerdBedrag = bedrag.ToString("N2", nlCulture);

        sb.Append("<tr style='border-bottom: 1px solid #ddd;'>");
        sb.Append($"<td>{item.clearingDate:HH:mm:ss}</td>");
        sb.Append($"<td>{item.MerchantName}</td>");
        sb.Append($"<td>{item.MerchantCode}</td>");
        sb.Append($"<td style='text-align: right;'>{geformatteerdBedrag}</td>"); // Rechts uitgelijnd voor bedragen
        sb.Append($"<td style='color: {statusKleur}; font-weight: bold;'>{item.StateText}</td></tr>");
    }

    sb.Append($"<tr style='font-weight: bold; background-color: #f9f9f9;'>");
    sb.Append($"<td colspan='3' style='text-align: right;'>Totaal:</td>");
    sb.Append($"<td style='text-align: right;'>{totaal.ToString("N2", nlCulture)}</td><td></td></tr>");
    sb.Append("</table>");

    return sb.ToString();
}

    private async Task VerstuurEmail(string htmlBody, int offset, CancellationToken ct = default)
{
    var settings = _configuration.GetSection("EmailSettings");
    var rapportDatum = DateTime.Now.AddDays(offset);

    string senderName = settings["SenderName"] ?? "PayNL Reporter";
    string senderEmail = settings["SenderEmail"] ?? throw new InvalidOperationException("SenderEmail mist.");
    string receiverEmail = settings["ReceiverEmail"] ?? throw new InvalidOperationException("ReceiverEmail mist.");
    string smtpServer = settings["SmtpServer"] ?? throw new InvalidOperationException("SmtpServer mist.");
    string password = settings["Password"] ?? ""; 

    var message = new MimeMessage();
    message.From.Add(new MailboxAddress(senderName, senderEmail));
    if (!string.IsNullOrEmpty(receiverEmail))
{
    // Splitst de adressen op basis van ; of ,
    var adressen = receiverEmail.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
    foreach (var adres in adressen)
    {
        message.To.Add(new MailboxAddress("Ontvanger", adres.Trim()));
    }
}
    message.Subject = $"Clearing reporter {rapportDatum:dd-MM-yyyy}";

    message.Body = new TextPart("html") { Text = htmlBody };

    // De 'using' zorgt dat de SMTP-client (en de onderliggende TCP-socket) 
    // direct wordt vrijgegeven na verzending.
    using (var client = new SmtpClient())
    {
        var useTls = bool.Parse(settings["UseTls"] ?? "true");
        var securityOptions = useTls ? MailKit.Security.SecureSocketOptions.StartTls : MailKit.Security.SecureSocketOptions.None;

        // We geven de 'ct' (CancellationToken) door aan elke async stap
        await client.ConnectAsync(smtpServer, int.Parse(settings["Port"] ?? "587"), securityOptions, ct);

        if (client.Capabilities.HasFlag(SmtpCapabilities.Authentication) && !string.IsNullOrEmpty(password))
        {
            await client.AuthenticateAsync(senderEmail, password, ct);
        }

        await client.SendAsync(message, ct);
        await client.DisconnectAsync(true, ct);
    }
}
}