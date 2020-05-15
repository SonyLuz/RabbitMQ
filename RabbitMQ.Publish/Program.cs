using RabbitMQ.Client;
using System;
using System.Threading;

namespace RabbitMQ.Publish
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
                #endregion

                //Criando conexão
                _connection = factory.CreateConnection();

                //Criando um canal para para envio e leitura de mensagem
                _channel = _connection.CreateModel();

                var exchangeName = "Minha_Exchange";
                var queueName = "Minha_Queue";
                                        //exchangeName, type, durable, exclusive, autodelete, arguments
                _channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout, false, false, null);
                                    //queueName, durable, exclusive, autodelete, arguments
                _channel.QueueDeclare(queueName, false, false, false, null);
                _channel.QueueBind(queueName, exchangeName, string.Empty, null);

                Console.WriteLine("Enviando msg");
                for (int i = 1; i <= 100; i++)
                {
                    //Enviando msg
                    PublishMsg(exchangeName, $"{i} - Olá Mundo!");
                    Thread.Sleep(1000);
                }
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

        public static void PublishMsg(string exchangeName, string msg)
        {       
            //Enviando msg
            Console.WriteLine(msg);
            byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes(msg);
            _channel.BasicPublish(exchangeName, string.Empty, null, messageBodyBytes);
            
        }
    }
}
