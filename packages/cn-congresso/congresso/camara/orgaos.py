#!/usr/bin/python
# -*- coding: latin-1 -*-

from ..connection import Connection
from ..utils import _make_url, _must_contain


class OrgaosClient(Connection):

    def listar_cargos_orgaos_legislativo(self):
        """
        Retorna a lista dos tipos de cargo para os �rg�os legislativos da C�mara dos Deputados (ex: presidente, primeiro-secret�rio, etc)

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ListarCargosOrgaosLegislativosCD

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/listarcargosorgaoslegislativoscd

        This method does not have input parameters
        """

        return self.perform_request('http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ListarCargosOrgaosLegislativosCD')

    def listar_tipos_orgaos(self):
        """
        Retorna a lista dos tipos de �rg�os que participam do processo legislativo na C�mara dos Deputados

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ListarTiposOrgaos

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/listartiposorgao

        This method does not have input parameters
        """

        return self.perform_request('http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ListarTiposOrgaos')

    def obter_andamento(self,
                        sigla=None,
                        numero=None,
                        ano=None,
                        dataIni=None,
                        codOrgao=None):
        """
        Retorna o andamento de uma proposi��o pelos �rg�os internos da C�mara a partir de uma data espec�fica

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterAndamento?sigla=PL&numero=3962&ano=2008&dataIni=01/01/2009&codOrgao=

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obterandamento

        Args:
            sigla:    String(Obrigatorio) :: Sigla do tipo de proposi��o
            numero:   Int(Obrigatorio)    :: Numero da proposi��o
            ano:      Int(Obrigatorio)    :: Ano da proposi��o
            dataIni:  String(Opcional)    :: Data a partir da qual as tramita��es do hist�rico de andamento ser�o retornadas (dd/mm/aaaa)
            codOrgao: String(Opcional)    :: ID do �rg�o numerador da proposi��o
        """
        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterAndamento?'
        params = dict([('sigla', sigla),
                       ('numero', numero),
                       ('ano', ano),
                       ('dataIni', dataIni),
                       ('codOrgao', codOrgao)])

        _must_contain(this=params, keys=['ano', 'numero', 'sigla'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def obter_emendas_substitutivo_redacao_final(self,
                                                 tipo=None,
                                                 ano=None,
                                                 numero=None):
        """
        Retorna as emendas, substitutivos e reda��es finais de uma determinada proposi��o

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterEmendasSubstitutivoRedacaoFinal?tipo=PL&numero=3962&ano=2008

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obteremendassubstitutivoredacaofinal

        Args:
            tipo:   String (Obrigatorio) :: Sigla do tipo de proposi��o
            ano:    Int (Obrigatorio) 	 :: Numero da proposi��o
            numero: Int (Obrigatorio) 	 :: Ano da proposi��o
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterEmendasSubstitutivoRedacaoFinal?'
        params = dict([('tipo', tipo),
                       ('numero', numero),
                       ('ano', ano)])

        _must_contain(this=params, keys=['ano', 'numero', 'tipo'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def obter_integra_comissoes_relator(self,
                                        tipo=None,
                                        ano=None,
                                        numero=None):
        """
        Retorna os dados de relatores e pareces, e o link para a �ntegra de uma determinada proposi��o

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterIntegraComissoesRelator?tipo=PL&numero=3962&ano=2008

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obterintegracomissoesrelator

        Args:
            tipo:   String (Obrigatorio) :: Sigla do tipo de proposi��o
            ano:    Int (Obrigatorio) 	 :: Numero da proposi��o
            numero: Int (Obrigatorio) 	 :: Ano da proposi��o
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterIntegraComissoesRelator?'
        params = dict([('tipo', tipo),
                       ('numero', numero),
                       ('ano', ano)])

        _must_contain(this=params, keys=['ano', 'numero', 'tipo'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))



    def obter_membros_orgaos(self, idOrgao=None):
        """
        Retorna os parlamentares membros de uma determinada comiss�o

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterMembrosOrgao?IDOrgao=2004

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obtermembrosorgao

        Args:
            idOrgao: Int (Obrigatorio)
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterMembrosOrgao?'
        params = dict([('idOrgao', idOrgao)])

        _must_contain(this=params, keys=['idOrgao'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))
    def obter_orgaos(self):
        """
        Retorna a lista de �rg�os legislativos da C�mara dos Deputados (comiss�es, Mesa Diretora, conselhos, etc.)

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterOrgaos

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obterorgaos

        This method does not have input parameters
        """

        return self.perform_request('http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterOrgaos')

    def obter_pauta(self,
                    idOrgao=None,
                    dataIni=None,
                    dataFim=None):
        """
        Retorna as pautas das reuni�es de comiss�es e das sess�es plen�rias realizadas em um determinado per�odo

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterPauta?IDOrgao=2004&datIni=01/01/2012&datFim=30/04/2012

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obterpauta

        Args:
            idOrgao: Int(Obrigatorio) 	ID do �rg�o (comiss�o) da C�mara dos Deputados
            dataIni: String(Opcional) 	O m�toto retorna a pauta das reuni�es que foram realizadas em uma data maior ou igual a datIni
            dataFim: String(Opcional) 	O m�toto retorna a pauta das reuni�es que foram realizadas em uma data menor ou igual a datFim
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterPauta?'
        params = dict([('idOrgao', idOrgao),
                       ('dataIni', dataIni),
                       ('dataFim', dataFim)])

        _must_contain(this=params, keys=['idOrgao'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))

    def obter_regime_tramitacao_despacho(self,
                                         tipo=None,
                                         numero=None,
                                         ano=None):
        """
        Retorna os dados do �ltimo despacho da proposi��o

        API ENDPOINT:
        http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterRegimeTramitacaoDespacho?tipo=PL&numero=8035&ano=2010

        API DOC:
        http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/orgaos/obterregimetramitacaodespacho

        Args:
            tipo:   String (Obrigatorio) ::	 Sigla do tipo de proposi��o
            numero: Int (Obrigatorio) 	 ::  Numero da proposi��o
            ano:    Int (Obrigatorio)    ::	 Ano da proposi��o
        """

        base_url = 'http://www.camara.gov.br/SitCamaraWS/Orgaos.asmx/ObterRegimeTramitacaoDespacho?'
        params = dict([('tipo', tipo),
                       ('numero', numero),
                       ('ano', ano)])

        _must_contain(this=params, keys=['tipo', 'ano', 'numero'])

        return self.perform_request(_make_url(api_house='camara',
                                              base_url=base_url,
                                              params=params))