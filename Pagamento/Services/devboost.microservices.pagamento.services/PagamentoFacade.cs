﻿using devboost.dronedelivery.core.domain;
using devboost.dronedelivery.core.domain.Entities;
using devboost.dronedelivery.core.domain.Enums;
using devboost.dronedelivery.pagamento.domain.Interfaces;
using devboost.dronedelivery.pagamento.EF.Integration.Interfaces;
using devboost.dronedelivery.pagamento.EF.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace devboost.dronedelivery.pagamento.services
{
    public class PagamentoFacade : IPagamentoFacade
    {
        private readonly IPagamentoRepository _pagamentoRepository;
        private readonly IPagamentoIntegration _pagamentoIntegration;

        public PagamentoFacade(IPagamentoRepository pagamentoRepository, IPagamentoIntegration pagamentoIntegration)
        {
            _pagamentoRepository = pagamentoRepository;
            _pagamentoIntegration = pagamentoIntegration;
        }

        public async Task<Pagamento> CriarPagamento(PagamentoCreateDto pagamento)
        {
            var newPagamento = new Pagamento
            {
                DadosPagamentos = pagamento.DadosPagamentos,
                TipoPagamento = pagamento.TipoPagamento,
                StatusPagamento = EStatusPagamento.EM_ANALISE,
                DataCriacao = DateTime.Now,
                Descricao = pagamento.Descricao
            };

            await _pagamentoRepository.AddAsync(newPagamento);

            return newPagamento;
        }

        private EStatusPagamento RandomPagamento()
        {
            return (EStatusPagamento)new Random().Next(1, 2);
        }


        public async Task<IEnumerable<PagamentoStatusDto>> VerificarStatusPagamentos()
        {
            var pagamentosResult = await _pagamentoRepository.GetPagamentosEmAnaliseAsync();

            List<PagamentoStatusDto> pagamentos = new List<PagamentoStatusDto>();

            foreach (var pagamento in pagamentosResult)
            {
                var status = RandomPagamento();

                var pagamentoStatusDto = new PagamentoStatusDto
                {
                    IdPagamento = pagamento.Id,
                    Status = status,
                    Descricao = pagamento.Descricao
                };

                pagamentos.Add(pagamentoStatusDto);

                AtualizarStatusPagamento(status, pagamento);
            }

            //Não será mais chamado o web hook da API de pedidos
            //if (await _pagamentoIntegration.ReportarResultadoAnalise(pagamentos))
            //    await _pagamentoRepository.SaveAsync();
            //else
            //    pagamentos = null;


            //Neste ponto, ao inves de chamar a API de pedidos para reportar o pagamento
            //Vamos passar a gravar um topico no Kafka informando o resultado

            if (await _pagamentoIntegration.EnviarPagamentoTopico(pagamentos))
                await _pagamentoRepository.SaveAsync();

            return pagamentos;
        }

        private void AtualizarStatusPagamento(EStatusPagamento eStatusPagamento, Pagamento pagamento)
        {
            pagamento.StatusPagamento = eStatusPagamento;
            _pagamentoRepository.SetState(pagamento, EntityState.Modified);
        }

    }
}
