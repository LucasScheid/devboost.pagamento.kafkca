using Confluent.Kafka;
using devboost.dronedelivery.core.domain;
using devboost.dronedelivery.felipe.Security.Entities;
using devboost.dronedelivery.security.domain.Entites;
using devboost.dronedelivery.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace job.verificador.pagamento
{
    class Program
    {
        static async Task Main(string[] args)
        {

            //Este JOB será resposnável por ler o tópico do KAFKCA e atualizar o pedido

            const string bootstrapServers = "localhost:9092";
            const string nomeTopic = "pedido-pagamento";
            const string finalUri = "http://localhost:5002/api/Pagamento";
            const string finalLogin = "http://localhost:5002/api/Login";

            Console.Clear();

            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine($"Consumindo mensagens do topico => {nomeTopic}");
            Console.WriteLine("");
            Console.WriteLine("");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"{nomeTopic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(nomeTopic);

                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Mensagem lida: {cr.Message.Value}");

                        if (!string.IsNullOrEmpty(cr.Message.Value))
                        {

                            var user = new LoginDTO()
                            {
                                UserId = "admin_drone",
                                Password = "AdminAPIDrone01!"
                            };

                            var request = new StringContent(JsonConvert.SerializeObject(user), Encoding.UTF8, "application/json");

                            using var client = new HttpClient();
                            var tokenJson = await client.PostAsync(finalLogin, request);
                            var token = JSONHelper.DeserializeJsonToObject<Token>(await tokenJson.Content.ReadAsStringAsync()) as Token;

                            var pagamentoStatusDto = JSONHelper.DeserializeJsonToObject<List<PagamentoStatusDto>>(cr.Message.Value);
                            var arraycontent = JSONHelper.ConvertObjectToByteArrayContent<List<PagamentoStatusDto>>(pagamentoStatusDto);

                            HttpClient httpClient = new HttpClient();
                            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.AccessToken);

                            var apiDeliveryResponse = await httpClient.PostAsync(finalUri, arraycontent);

                            Console.WriteLine($"API de pagamento chamada OK? =>  {apiDeliveryResponse.IsSuccessStatusCode}");
                        }

                        Thread.Sleep(3000);

                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                    Console.WriteLine("Cancelando a execução do consumidor...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " + $"Mensagem: {ex.Message}");
            }
        }

    }
}
