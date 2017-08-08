#!/usr/bin/python
# -*- coding: latin-1 -*-
from ..connection import Connection
from ..utils import _make_url, _must_contain


class ProposicoesClient(Connection):

    def listar_proposicoes(self,
                           sigla=None,
                           ano=None,
                           numero=None,
                           datApresentacaoIni=None,
                           datApresentacaoFim=None,
                           idTipoAutor=None,
                           parteNomeAutor=None,
                           siglaPartidoAutor=None,
                           siglaUfAutor=None,
                           generoAutor=None,
                           codEstado=None,
                           codOrgaoEstado=None,
                           emTramitacao=None
                           ):
        """
        Retorna a lista de proposi��es que satisfa�am os crit�rios estabelecidos

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoes?sigla=PL&
(                                                                codEstado=&
                                                                                codOrgaoEstado=&
                                                                                emTramitacao=

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarproposicoes

        Args:
            sigla: 	                    String(Obrigatorio se ParteNomeAutor n�o for preenchido) ::	Sigla do tipo de proposi��o
            ano: 	                    Int(Obrigatorio se ParteNomeAutor n�o for preenchido) 	 :: Ano da proposi��o

            numero: 	                Int(Obrigatorio)    :: Numero da proposi��o
            datApresentacaoIni: 	    Date(Opcional) 	 :: Menor data desejada para a data de apresenta��o da proposi��o.
                                                            Formato: DD/MM/AAAA
            datApresentacaoFim: 	    Date(Opcional) 	 :: Maior data desejada para a data de apresenta��o da proposi��o
                                                            Formato: DD/MM/AAAA
            idTipoAutor: 	            Int(Optional) 	 :: Identificador do tipo de �rg�o autor da proposi��o,
                                                            como obtido na chamada ao ListarTiposOrgao
            parteNomeAutor: 	        String(Optional) :: Parte do nome do autor(5 ou + caracteres) da proposi��o.
            siglaPartidoAutor: 	        String(Optional) :: Sigla do partido do autor da proposi��o
            siglaUfAutor: 	            String(Optional) :: UF de representa��o do autor da proposi��o
            generoAutor: 	            String(Optional) :: G�nero do autor<BR>M - Masculino; F - Feminino;
                                                            Default - Todos
            emTramitacao: 	            int(Opcional) 	 :: Indicador da situa��o de tramita��o da proposi��o
                                                            1 - Em Tramita��o no Congresso;
                                                            2- Tramita��o Encerrada no Congresso;
                                                            Default - Todas
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoes?'
        params = dict([('sigla', sigla),
                        ('ano', ano),
                        ('numero', numero),
                        ('datApresentacaoIni', datApresentacaoIni),
                        ('datApresentacaoFim', datApresentacaoFim),
                        ('idTipoAutor', idTipoAutor),
                        ('parteNomeAutor', parteNomeAutor),
                        ('siglaPartidoAutor', siglaPartidoAutor),
                        ('siglaUfAutor', siglaUfAutor),
                        ('generoAutor', generoAutor),
                        ('codEstado', codEstado),
                        ('codOrgaoEstado', codOrgaoEstado),
                        ('emTramitacao', emTramitacao)])

        try:
            _must_contain(params, ['parteNomeAutor'])
        except AttributeError:
            _must_contain(params, ['sigla', 'ano'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def listar_siglas_tipo_proposicao(self):
        """
        Retorna a lista de siglas de proposi��es

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarSiglasTipoProposicao

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarsiglastipoproposicao

        This method does not have input parameters
        """

        return self.perform_request('http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarSiglasTipoProposicao')

    def listar_situacoes_proposicao(self):
        """
        Retorna a lista de situa��es para proposi��es

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarSituacoesProposicao

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarsituacoesproposicao

        This method does not have input parameters
        """

        return  self.perform_request('http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarSituacoesProposicao')

    def listar_tipos_autores(self):
        """
        Retorna a lista de situa��es para proposi��es

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarTiposAutores

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listartiposautores

        This method does not have input parameters
        """

        return self.perform_request('http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarTiposAutores')

    def obter_proposicao(self,
                         tipo=None,
                         ano=None,
                         numero=None,
                         idProp=None):
        """
        Retorna os dados de uma determinada proposi��o

        Grupo I
        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterProposicao?tipo=PL&numero=3962&ano=2008

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/obterproposicao

        Grupo II
        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterProposicaoPorID?IdProp=354258

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/obterproposicaoporid

        Args:
            Completar ou Grupo I ou Grupo II

            Grupo I
            tipo:   String (Obrigatorio) ::	Sigla do tipo de proposi��o
            ano:    Int (Obrigatorio) 	 :: Numero da proposi��o
            numero: Int (Obrigatorio) 	 :: Ano da proposi��o

            Grupo II
            idProp: Int (Obrigatorio)    ::	ID da proposi��o desejada
        """

        params = dict([('tipo', tipo),
                       ('ano', ano),
                       ('numero', numero),
                       ('idProp', idProp),
                       ])

        try:
            _must_contain(params, ['tipo', 'ano', 'numero'])
            base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterProposicao?'
            return self.perform_request(_make_url(api_house='camara',
                                                  base_url=base_url,
                                                  params=params))

        except AttributeError:
            _must_contain(params, ['idProp'])
            base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterProposicaoPorID?'
            return self.perform_request(_make_url(api_house='camara',
                                                  base_url=base_url,
                                                  params=params))



    def obter_proposicao_votacao(self,
                                 tipo=None,
                                 ano=None,
                                 numero=None):
        """
        Retorna os votos dos deputados a uma determinada proposi��o em vota��es ocorridas no Plen�rio da C�mara dos Deputados

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterVotacaoProposicao?tipo=PL&numero=1992&ano=2007

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/obtervotacaoproposicao

        Args:
            tipo:   String (Obrigatorio) ::	  Sigla do tipo de proposi��o
            ano:    Int (Obrigatorio) 	 ::   Numero da proposi��o
            numero: Int (Obrigatorio) 	 ::   Ano da proposi��o
        """
        base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoes?'
        params = dict([('tipo', tipo),
                       ('ano', ano),
                       ('numero', numero),
                       ])

        _must_contain(params, ['tipo', 'ano', 'numero'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def listar_proposicoes_votadas_em_plenario(self,
                                               ano=None,
                                               tipo=None):
        """
        Retorna a lista de proposi��es que sofreram vota��o em plen�rio em determinado ano.

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesVotadasEmPlenario?ano=2013&tipo=

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/ProposicoesVotadasEmPlenario

        Args:
            ano:  int(Obrigatorio) ::	Ano da proposi��o
            tipo: String(Opcional) ::	Tipo de proposi��o
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesVotadasEmPlenario?'
        params = dict([('tipo', tipo),
                       ('ano', ano),
                       ])

        _must_contain(params, ['ano'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def listar_proposicoes_tramitadas_no_periodo(self,
                                                 dtInicio=None,
                                                 dtFim=None):
        """
        Retorna lista de proposi��es que tramitaram em determinado per�odo. O per�odo m�ximo � de 7 dias

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesTramitadasNoPeriodo?dtInicio=20/09/2013&dtFim=21/09/2013

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarProposicoesTramitadasNoPeriodo

        Args:
            dtInicio: String (Obrigatorio) ::	Data de in�cio
            dtFim:    String (Obrigatorio) ::	Data final
        """
        base_url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesTramitadasNoPeriodo?'
        params = dict([('dtInicio', dtInicio),
                       ('dtFim', dtFim)])

        _must_contain(params, ['dtInicio', 'dtFim'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))