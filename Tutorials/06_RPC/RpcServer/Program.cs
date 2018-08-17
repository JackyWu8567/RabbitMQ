using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RpcServer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { };

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);
                    channel.BasicConsume(queue: "rpc_queue", autoAck: false, consumer: consumer);
                    Console.WriteLine(" [x] Awaiting RPC requests");

                    consumer.Received += (model, eventArgs) => 
                    {
                        string response = null;

                        var body = eventArgs.Body;
                        var replyTo = eventArgs.BasicProperties.ReplyTo;

                        var repProps = channel.CreateBasicProperties();
                        repProps.CorrelationId = eventArgs.BasicProperties.CorrelationId;

                        try
                        {
                            var message = Encoding.UTF8.GetString(body);
                            int n = int.Parse(message);
                            Console.WriteLine(" [.] fib({0})", message);
                            response = fib(n).ToString();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(" [.] " + e.Message);
                            response = "";
                        }
                        finally
                        {
                            var responseBytes= Encoding.UTF8.GetBytes(response);
                            channel.BasicPublish(exchange: "", routingKey: replyTo, basicProperties: repProps, body: responseBytes);
                            channel.BasicAck(deliveryTag: eventArgs.DeliveryTag, multiple: false);
                        }
                    };

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        private static int fib(int n)
        {
            if (n == 0 || n == 1)
            {
                return n;
            }

            return fib(n - 1) + fib(n - 2);
        }
    }
}
