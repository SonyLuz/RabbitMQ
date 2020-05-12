using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.Consumer
{
    class Program
    {
        public static IConnection _connection;
        public static IModel _channel;

        static void Main(string[] args)
        {
            try
            {
                #region Definindo conexão com o rabbit
                ConnectionFactory factory = new ConnectionFactory();
                factory.UserName = "rabbitmq";
                factory.Password = "rabbitmq";
                factory.VirtualHost = "development";
                factory.HostName = "localhost";
                factory.Port = 5672;
                factory.ContinuationTimeout = TimeSpan.FromMinutes(1);
                factory.RequestedHeartbeat = TimeSpan.FromSeconds(300);
                #endregion

                //Criando conexão
                _connection = factory.CreateConnection();

                //Criando um canal para para envio e leitura de mensagem
                _channel = _connection.CreateModel();
                var queueName = "Minha_Queue";

                ConsumerMsg(queueName);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                //Fechando canal
                _channel.Close();

                //fechando conexão
                _connection.Close();
            }

        }

        public static void ConsumerMsg(string queueName)
        {
            bool ready = true;
            Console.WriteLine("Iniciando Consumo da fila");
            int count = 0;
            while (ready)
            {
                //get msg
                var model = _channel.BasicGet(queueName, false);

                if (model == null)
                {
                    Thread.Sleep(5000);
                }
                else
                {
                    count++;
                    //get msg array bytes
                    string msg = Encoding.UTF8.GetString(model.Body.ToArray());
                    Console.WriteLine($"{count}- Mensagem recuperada: {msg}");

                    // executa ack para remoção da msg da fila
                    _channel.BasicAck(model.DeliveryTag, false);
                }
            }

        }
    }
}
