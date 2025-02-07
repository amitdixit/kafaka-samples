using Confluent.Kafka;


//await SingleTopicMultiPartition();

//await ConfluentProducer();

await Notifications();


async Task Notifications()
{
    var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

    using (var producer = new ProducerBuilder<Null, string>(config).Build())
    {
        for (int i = 0; i < 10; i++)
        {
            var message = new Message<Null, string> { Value = $"Notification {i}" };
            await producer.ProduceAsync("notifications", message);
            Console.WriteLine($"Produced: {message.Value}");
        }
    }
}


async Task ConfluentProducer()
{
    const string topic = "test-topic";

    string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
    string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        Acks = Acks.All,
    };

    using (var producer = new ProducerBuilder<string, string>(config).Build())
    {
        var numProduced = 0;
        Random rnd = new Random();
        const int numMessages = 10;
        for (int i = 0; i < numMessages; ++i)
        {
            var user = users[rnd.Next(users.Length)];
            var item = items[rnd.Next(items.Length)];

            producer.Produce(topic, new Message<string, string> { Key = user, Value = item },
                 (deliveryReport) =>
                 {
                     if (deliveryReport.Error.Code != ErrorCode.NoError)
                     {
                         Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                     }
                     else
                     {
                         Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                         numProduced += 1;
                     }
                 });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
    }
}



async Task SingleTopicMultiPartition()
{

    var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

    var topicPartition = new TopicPartition("test-topic", new Partition(3));

    using (var producer = new ProducerBuilder<string, string>(config).Build())
    {
        for (int i = 0; i < 10; i++)
        {
            string key = (i % 3).ToString(); // Different keys will be mapped to different partitions
            string value = $"Message {i} with Key {key}";

            var deliveryReport = await producer.ProduceAsync("my-topic", new Message<string, string>
            {
                Key = key,
                Value = value
            });

            Console.WriteLine($"Sent to Partition {deliveryReport.Partition}: {deliveryReport.Value}");
        }
    }
}
void SingleTopicSinglePartition()
{
    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };

    using (var producer = new ProducerBuilder<Null, string>(config).Build())
    {
        try
        {
            // Produce a message to the topic "test-topic"
            var dr = producer.ProduceAsync("test-topic", new Message<Null, string> { Value = "Hello Kafka from C#" }).Result;
            Console.WriteLine($"Message sent: {dr.Value}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Error sending message: {e.Error.Reason}");
        }
    }
}