using System.Text;
using System.Text.Json;
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

        if (runOnStartup)
        {
            _logger.LogInformation("Test-run: Directe uitvoering bij opstarten...");
            await HaalDataEnVerstuurEmail(stoppingToken);
            _lastRunDate = DateTime.Now;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var nu = DateTime.Now;
            var tijdNu = nu.ToString("HH:mm");

            if (tijdNu == scheduledTime && _lastRunDate.Date != nu.Date)
            {
                _logger.LogInformation("Starten van geplande dagelijkse rapportage...");
                await HaalDataEnVerstuurEmail(stoppingToken);
                _lastRunDate = nu;
                _logger.LogInformation("Rapportage voltooid. Volgende run gepland voor morgen om {time}.", scheduledTime);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
            catch (OperationCanceledException)
            {
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
            var rapportDatum = DateTime.Now.AddDays(dayOffset);

            var connString = _configuration.GetConnectionString("DefaultConnection");

            // Logbestand éénmalig inlezen voor de rapportdatum
            string[] logRegels = await LeesLogBestand(settings["LogFilePath"], rapportDatum, ct);

            using var connection = new SqlConnection(connString);

            var queryNieuw = @"
                SELECT
                    id,
                    apiCallDate,
                    clearingDate,
                    MerchantName,
                    MerchantCode,
                    amount,
                    state,
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

            var queryRetry = @"
                SELECT
                    id,
                    apiCallDate,
                    clearingDate,
                    MerchantName,
                    MerchantCode,
                    amount,
                    state,
                    CASE state
                        WHEN 0 THEN 'Created'
                        WHEN 1 THEN 'Pending'
                        WHEN 2 THEN 'Succesfull'
                        WHEN 3 THEN 'Failed'
                        ELSE 'Unknown'
                    END AS StateText
                FROM Clearings
                WHERE CAST(apiCallDate AS DATE) = CAST(DATEADD(day, @Offset, GETDATE()) AS DATE)
                  AND CAST(clearingDate AS DATE) < CAST(DATEADD(day, @Offset, GETDATE()) AS DATE)
                ORDER BY clearingDate DESC";

            var nieuweData = (await connection.QueryAsync(new CommandDefinition(queryNieuw, new { Offset = dayOffset }, cancellationToken: ct))).ToList();
            var retryData  = (await connection.QueryAsync(new CommandDefinition(queryRetry,  new { Offset = dayOffset }, cancellationToken: ct))).ToList();

            var emailBody = BouwHtmlBody(nieuweData, retryData, dayOffset, logRegels);
            await VerstuurEmail(emailBody, dayOffset, ct);

            _logger.LogInformation("E-mail succesvol verzonden voor offset {Offset}.", dayOffset);
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

    private async Task<string[]> LeesLogBestand(string? logPad, DateTime datum, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(logPad))
            return [];

        var bestandsnaam = $"logfile-PayNlManager-{datum:dd-MM-yyyy}.txt";
        var volledigPad = Path.Combine(logPad, bestandsnaam);

        if (!File.Exists(volledigPad))
        {
            _logger.LogWarning("Logbestand niet gevonden: {pad}", volledigPad);
            return [];
        }

        _logger.LogInformation("Logbestand ingelezen: {pad}", volledigPad);

        // FileShare.ReadWrite: veilig lezen terwijl PAYNL Manager het bestand open heeft
        using var stream = new FileStream(volledigPad, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);
        var inhoud = await reader.ReadToEndAsync(ct);
        return inhoud.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
    }

    private static string? LeesReden(string[] logRegels, string clearingId)
    {
        const string zoekterm = "API RESULT =>";
        var idZoekterm = $"Do Clearing Id = {clearingId}";

        for (int i = 0; i < logRegels.Length - 1; i++)
        {
            if (!logRegels[i].Contains(idZoekterm))
                continue;

            // ID gevonden — zoek in de volgende regels naar API RESULT =>
            for (int j = i + 1; j < Math.Min(i + 5, logRegels.Length); j++)
            {
                if (!logRegels[j].Contains(zoekterm))
                    continue;

                var jsonStart = logRegels[j].IndexOf(zoekterm) + zoekterm.Length;
                var json = logRegels[j][jsonStart..].Trim();

                try
                {
                    using var doc = JsonDocument.Parse(json);
                    if (doc.RootElement.TryGetProperty("request", out var request) &&
                        request.TryGetProperty("errorMessage", out var errorMessage))
                    {
                        return errorMessage.GetString();
                    }
                }
                catch
                {
                    // Ongeldige JSON in logfile — reden overslaan
                }
            }
        }

        return null;
    }

    private string BouwHtmlBody(List<dynamic> nieuweData, List<dynamic> retryData, int offset, string[] logRegels)
    {
        var nlCulture = new CultureInfo("nl-NL");
        var rapportDatum = DateTime.Now.AddDays(offset);
        var sb = new StringBuilder();

        sb.Append("<h2 style='font-family: Arial, sans-serif; color: #333;'>Dagelijks Clearing Overzicht</h2>");
        sb.Append($"<p style='font-family: Arial, sans-serif;'>Rapport voor datum: {rapportDatum:dd-MM-yyyy}</p>");

        sb.Append("<h3 style='font-family: Arial, sans-serif; color: #004a99;'>Retry Clearings</h3>");
        BouwTabel(sb, retryData, nlCulture, logRegels, "GEEN RETRY CLEARINGS VOOR VANDAAG");

        sb.Append("<h3 style='font-family: Arial, sans-serif; color: #004a99; margin-top: 32px;'>Nieuwe Clearings</h3>");
        BouwTabel(sb, nieuweData, nlCulture, logRegels, "GEEN CLEARINGS VOOR VANDAAG");

        return sb.ToString();
    }

    private static void BouwTabel(StringBuilder sb, List<dynamic> data, CultureInfo nlCulture, string[] logRegels, string leegTekst)
    {
        if (!data.Any())
        {
            sb.Append("<table border='0' cellpadding='8' style='border-collapse: collapse; font-family: Arial, sans-serif; width: 100%;'>");
            sb.Append("<tr><td style='text-align: center; padding: 40px; color: red; font-size: 20px; font-weight: bold;'>");
            sb.Append(leegTekst);
            sb.Append("</td></tr></table>");
            return;
        }

        sb.Append("<table border='0' cellpadding='8' style='border-collapse: collapse; font-family: Arial, sans-serif; width: 100%; min-width: 700px;'>");
        sb.Append("<tr style='background-color: #004a99; color: white; text-align: left;'>");
        sb.Append("<th>Tijd</th><th>MerchantCode</th><th>Merchant</th><th>Clearingdatum</th><th>Bedrag</th><th>Status</th><th>Reden</th></tr>");

        decimal totaal = 0;
        foreach (var item in data)
        {
            decimal bedrag = (decimal)(item.amount ?? 0m);
            totaal += bedrag;

            bool isFailed = (int)(item.state ?? 0) == 3;
            string statusKleur = item.StateText == "Succesfull" ? "#28a745" : (isFailed ? "#dc3545" : "#333");
            string geformatteerdBedrag = bedrag.ToString("N2", nlCulture);

            string reden = "";
            if (isFailed)
                reden = LeesReden(logRegels, ((object)item.id).ToString()!) ?? "";

            sb.Append("<tr style='border-bottom: 1px solid #ddd;'>");
            sb.Append($"<td>{item.apiCallDate:HH:mm:ss}</td>");
            sb.Append($"<td>{item.MerchantCode}</td>");
            sb.Append($"<td>{item.MerchantName}</td>");
            sb.Append($"<td>{item.clearingDate:dd-MM-yyyy}</td>");
            sb.Append($"<td style='text-align: right;'>{geformatteerdBedrag}</td>");
            sb.Append($"<td style='color: {statusKleur}; font-weight: bold;'>{item.StateText}</td>");
            sb.Append($"<td style='color: #dc3545;'>{reden}</td>");
            sb.Append("</tr>");
        }

        sb.Append("<tr style='font-weight: bold; background-color: #f9f9f9;'>");
        sb.Append("<td colspan='4' style='text-align: right;'>Totaal:</td>");
        sb.Append($"<td style='text-align: right;'>{totaal.ToString("N2", nlCulture)}</td><td></td><td></td></tr>");
        sb.Append("</table>");
    }

    private async Task VerstuurEmail(string htmlBody, int offset, CancellationToken ct = default)
    {
        var settings = _configuration.GetSection("EmailSettings");
        var rapportDatum = DateTime.Now.AddDays(offset);

        string senderName    = settings["SenderName"]    ?? "PayNL Reporter";
        string senderEmail   = settings["SenderEmail"]   ?? throw new InvalidOperationException("SenderEmail mist.");
        string receiverEmail = settings["ReceiverEmail"] ?? throw new InvalidOperationException("ReceiverEmail mist.");
        string smtpServer    = settings["SmtpServer"]    ?? throw new InvalidOperationException("SmtpServer mist.");
        string password      = settings["Password"]      ?? "";

        var message = new MimeMessage();
        message.From.Add(new MailboxAddress(senderName, senderEmail));

        var adressen = receiverEmail.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var adres in adressen)
            message.To.Add(new MailboxAddress("Ontvanger", adres.Trim()));

        message.Subject = $"Clearing reporter {rapportDatum:dd-MM-yyyy}";
        message.Body = new TextPart("html") { Text = htmlBody };

        using var client = new SmtpClient();

        var useTls = bool.Parse(settings["UseTls"] ?? "true");
        var securityOptions = useTls ? MailKit.Security.SecureSocketOptions.StartTls : MailKit.Security.SecureSocketOptions.None;

        await client.ConnectAsync(smtpServer, int.Parse(settings["Port"] ?? "587"), securityOptions, ct);

        if (client.Capabilities.HasFlag(SmtpCapabilities.Authentication) && !string.IsNullOrEmpty(password))
            await client.AuthenticateAsync(senderEmail, password, ct);

        await client.SendAsync(message, ct);
        await client.DisconnectAsync(true, ct);
    }
}
