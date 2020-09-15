using Confluent.Kafka;
using devboost.dronedelivery.core.domain;
using devboost.dronedelivery.core.services;
using devboost.dronedelivery.domain.Constants;
using devboost.dronedelivery.pagamento.EF.Integration.Interfaces;
using devboost.dronedelivery.Services;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace devboost.dronedelivery.pagamento.EF.Integration
{
    public class PagamentoIntegration : IPagamentoIntegration
    {
        private readonly string _urlBase;
        private readonly HttpService _httpService;
        public PagamentoIntegration(DeliverySettingsData deliverySettings, HttpService httpService)
        {
            _urlBase = deliverySettings.UrlBase;
            _httpService = httpService;
        }


        public async Task<bool> ReportarResultadoAnalise(List<PagamentoStatusDto> listaPagamentos)
        {
            string finalUri = string.Concat(_urlBase, ProjectConsts.DELIVERY_URI);

            var apiDeliveryResponse = await _httpService.PostAsync(finalUri, JSONHelper.ConvertObjectToByteArrayContent<List<PagamentoStatusDto>>(listaPagamentos));

            return apiDeliveryResponse.IsSuccessStatusCode;

        }

        public async Task<bool> EnviarPagamentoTopico(List<PagamentoStatusDto> pagamentos)
        {
            //To-Do
            //Remover isso também depois, deixar fazendo via injeção de dependencia

            const string bootstrapServers = "localhost:9092";
            const string nomeTopic = "pedido-pagamento";

            string payload = Newtonsoft.Json.JsonConvert.SerializeObject(pagamentos);

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using var producer = new ProducerBuilder<Null, string>(config).Build();

                var result = await producer.ProduceAsync(
                    nomeTopic,
                    new Message<Null, string>
                    { Value = payload });

                return true;

            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
