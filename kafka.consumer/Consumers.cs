using Confluent.Kafka;

class Consumers
{
    public static void SmsConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "sms-service",  // Different group
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("notifications");

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"📩 SMS Consumer received: {consumeResult.Message.Value}");
            }
        }
    }

    public static void EmailConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "email-service",  // Different group
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("notifications");

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"📧 Email Consumer received: {consumeResult.Message.Value}");
            }
        }
    }

    public static void LoggingConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "logging-service",  // Different group
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("notifications");

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"📜 Logging Consumer received: {consumeResult.Message.Value}");
            }
        }
    }
}