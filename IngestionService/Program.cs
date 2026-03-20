using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// RabbitMQ connection settings
var factory = new ConnectionFactory { 
    HostName = "localhost", 
    UserName = "admin", 
    Password = "admin123" 
};

// Default exchange where RabbitMQ's MQTT plugin publishes messages
string exchangeName = "amq.topic";
// Using '#' as a wildcard. This means "Get everything starting with 'prod.'"
string routingKey = "prod.#";
string queueName = "telemetry_ingestion_queue";

// Establishing connection and creating the channel
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

// Declaring the queue and binding it to the exchange
channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: routingKey);

Console.WriteLine("[*] Ingestion Service started (C# .NET).");
Console.WriteLine($"[*] Waiting for telemetry in queue '{queueName}'. To exit press CTRL+C");

// Configuring the consumer
var consumer = new EventingBasicConsumer(channel);

consumer.Received += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    var routingKeyStr = ea.RoutingKey;

    try
    {
        // Deserializing the strongly typed JSON
        var payload = JsonSerializer.Deserialize<TelemetryPayload>(message);

        if (payload != null)
        {
            Console.WriteLine($"\nNew reading received from: {routingKeyStr}");
            Console.WriteLine($" ├── Device:       {payload.DeviceId}");
            Console.WriteLine($" ├── Temperature:  {payload.Data.Temperature:F2} °C");
            Console.WriteLine($" ├── Battery:      {payload.Metrics.BatteryV} V");
            Console.WriteLine($" └── Wi-Fi Signal: {payload.Metrics.WifiRssi} dBm");
            
            // Here, in the future, is where we will send data to the Database!
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n[❌] Error processing message: {ex.Message}");
    }
};

// Telling the channel to start consuming
channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

// Keeps the application running
Console.ReadLine();

// DATA MODELS


public record TelemetryPayload(
    [property: JsonPropertyName("device_id")] string DeviceId,
    [property: JsonPropertyName("timestamp")] long Timestamp,
    [property: JsonPropertyName("data")] TelemetryData Data,
    [property: JsonPropertyName("metrics")] TelemetryMetrics Metrics
);

public record TelemetryData(
    [property: JsonPropertyName("temperature")] double Temperature
);

public record TelemetryMetrics(
    [property: JsonPropertyName("battery_v")] double BatteryV,
    [property: JsonPropertyName("wifi_rssi")] int WifiRssi
);
