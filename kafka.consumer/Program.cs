using Confluent.Kafka;

//ConsumeFromMultiplePartition();

//await ConfluentConsumer();


var tasks = new List<Task>
{
    Task.Run(() => Consumers.SmsConsumer()),
    Task.Run(() => Consumers.EmailConsumer()),
    Task.Run(() => Consumers.LoggingConsumer())
};


await Task.WhenAll(tasks);



void ConsumeFromMultiplePartition()
{
    const string topic = "test-topic";
    var config = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "test-consumer-group",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
    {
        // consumer.Assign(new TopicPartition("test-topic", new Partition(1))); // Consume only from partition 1
        consumer.Subscribe(topic);
        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Consumed from Partition {consumeResult.Partition}: {consumeResult.Message.Value}");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error: {e.Error.Reason}");
        }
    }
}


async Task ConfluentConsumer()
{
    var config = new ConsumerConfig
    {
        // User-specific properties that you must set
        BootstrapServers = "localhost:9092",
        GroupId = "kafka-dotnet-getting-started",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    const string topic = "test-topic";

    CancellationTokenSource cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true; // prevent the process from terminating.
        cts.Cancel();
    };

    using (var consumer = new ConsumerBuilder<string, string>(config).Build())
    {
        consumer.Subscribe(topic);
        try
        {
            while (true)
            {
                var cr = consumer.Consume(cts.Token);
                Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C was pressed.
        }
        finally
        {
            consumer.Close();
        }
    }
}



void ConsumeFromSinglePartition()
{
    var config = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "test-consumer-group",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
    {
        consumer.Subscribe("test-topic");

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Consumed message: {consumeResult.Message.Value}");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error: {e.Error.Reason}");
        }
    }
}
