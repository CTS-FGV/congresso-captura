#!/usr/bin/python
# -*- coding: utf-8 -*-

######################################################################################################
# ATENÇÃO: reparar que está salvando diversas vezes o mesmo cdeputado por causa do hstórico.
# Ex: um deputado que tenha passado por 3 partidos aparecerá 3 vezes, uma em cada partido.
# É preciso ver se é possível pegar pelo menos a data em que houve a alteração.
# Por enquanto é possível resolver com um DISTINCT ON () nos selects.
#
######################################################################################################


import psycopg2
import time
import datetime
import sys
import urllib2
from lxml import etree

def connect():
    print 'conn'
    conn = psycopg2.connect(
        host='172.16.4.229', port=5432, database='cts',
        user='cts', password='senhaCTS2017_')
    return conn

def main():

    # Connect
    conn = connect()
    cursor = conn.cursor()

    sql = "set datestyle to SQL,DMY;"
    cursor.execute(sql)

    # LOG
    cursor.execute('SELECT MAX(capnum) FROM c_camdep.camdep_capture_log')
    maxcapnum = cursor.fetchall()
    nextcapnum = maxcapnum[0][0]+1
    script = "cap_camdep_deputados_detalhes.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Listando deputados a capturar"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    sql = """
            SELECT DISTINCT idecadastro FROM (
            SELECT idecadastro
            FROM c_camdep.camdep_deputados_atual
            UNION
            SELECT idecadastro
            FROM c_camdep.camdep_deputados_historico
            ) tmp
            ORDER BY 1
        """
    cursor.execute(sql)
    idecadastro = cursor.fetchall()

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida listagem de deputados a capturar"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    counter = 0

    for dep in idecadastro:

        # Inicia captura de XML
        xml = urllib2.urlopen('http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro={0}&numLegislatura='.format(dep[0]))
        parser = etree.parse(xml)

        for i in parser.findall('Deputado'):

            p = [i.find(n).text for n in ('numLegislatura', 'email', 'nomeProfissao',
                                          'dataNascimento', 'dataFalecimento', 'ufRepresentacaoAtual',
                                          'situacaoNaLegislaturaAtual', 'ideCadastro', 'idParlamentarDeprecated', 'nomeParlamentarAtual', 'nomeCivil',
                                          'sexo'
                                          )]

            # Partido
            for i2 in i.findall('partidoAtual'):
                p2 = [i2.find(n).text for n in ('idPartido', 'sigla', 'nome')]
                p.extend(p2)

            # Gabinete
            for i2 in i.findall('gabinete'):
                p2 = [i2.find(n).text for n in ('numero', 'anexo', 'telefone')]
                p.extend(p2)

            ###################################
            # COMISSOES
            ###################################
            for comissoes in i.findall('comissoes'):
                for comissao in comissoes.findall('comissao'):
                    comissao_detalhes = [comissao.find(n).text for n in ('idOrgaoLegislativoCD', 'siglaComissao', 'nomeComissao', 'condicaoMembro', 'dataEntrada', 'dataSaida')]
                    for x in range(len(comissao_detalhes)):
                        if comissao_detalhes[x]:
                            comissao_detalhes[x] = comissao_detalhes[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                        else:
                            comissao_detalhes[x] = ''

                    #print comissao_detalhes

                    # Arruma aspas e nulls
                    comissao_detalhes[0]    =     comissao_detalhes[0]              if len(comissao_detalhes[0]) > 0 else "NULL"            # idOrgaoLegislativoCD::int
                    comissao_detalhes[1]    = "'"+comissao_detalhes[1]+"'"          if len(comissao_detalhes[1]) > 0 else "NULL"            # siglaComissao::char
                    comissao_detalhes[2]    = "'"+comissao_detalhes[2]+"'"          if len(comissao_detalhes[2]) > 0 else "NULL"            # nomeComissao::char
                    comissao_detalhes[3]    = "'"+comissao_detalhes[3]+"'"          if len(comissao_detalhes[3]) > 0 else "NULL"            # condicaoMembro::char
                    comissao_detalhes[4]    = "'"+comissao_detalhes[4]+"'"          if len(comissao_detalhes[4]) > 0 else "NULL"            # dataEntrada::date
                    comissao_detalhes[5]    = "'"+comissao_detalhes[5]+"'"          if len(comissao_detalhes[5]) > 0 else "NULL"            # dataSaida::date

                    sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes_comissoes ("
                        "capnum, datacaptura, "
                        "numlegislatura, idecadastro, "
                        "idorgaolegislativocd, siglacomissao, nomecomissao, "
                        "condicaomembro, dataentrada, datasaida "
                        ") VALUES ("
                        "{0}, '{1}', "
                        "{2}, {3}, "
                        "{4}, {5}, {6}, "
                        "{7}, {8}, {9} "
                        ")").format(nextcapnum, st, p[0], p[7], comissao_detalhes[0], comissao_detalhes[1], comissao_detalhes[2], comissao_detalhes[3], comissao_detalhes[4], comissao_detalhes[5])
                    #print sql
                    cursor.execute(sql)

            ###################################
            # CARGO COMISSOES
            ###################################
            for cargo_comissoes in i.findall('cargosComissoes'):
                for cargo_comissao in cargo_comissoes.findall('cargoComissoes'):
                    cargo_comissao_detalhes = [cargo_comissao.find(n).text for n in ('idOrgaoLegislativoCD', 'siglaComissao', 'nomeComissao', 'idCargo', 'nomeCargo', 'dataEntrada', 'dataSaida')]
                    for x in range(len(cargo_comissao_detalhes)):
                        if cargo_comissao_detalhes[x]:
                            cargo_comissao_detalhes[x] = cargo_comissao_detalhes[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                        else:
                            cargo_comissao_detalhes[x] = ''

                    #print cargo_comissao_detalhes

                    # Arruma aspas e nulls
                    cargo_comissao_detalhes[0]    =     cargo_comissao_detalhes[0]              if len(cargo_comissao_detalhes[0]) > 0 else "NULL"            # idOrgaoLegislativoCD::int
                    cargo_comissao_detalhes[1]    = "'"+cargo_comissao_detalhes[1]+"'"          if len(cargo_comissao_detalhes[1]) > 0 else "NULL"            # siglaComissao::char
                    cargo_comissao_detalhes[2]    = "'"+cargo_comissao_detalhes[2]+"'"          if len(cargo_comissao_detalhes[2]) > 0 else "NULL"            # nomeComissao::char
                    cargo_comissao_detalhes[3]    =     cargo_comissao_detalhes[3]              if len(cargo_comissao_detalhes[3]) > 0 else "NULL"            # idCargo::int
                    cargo_comissao_detalhes[4]    = "'"+cargo_comissao_detalhes[4]+"'"          if len(cargo_comissao_detalhes[4]) > 0 else "NULL"            # nomeCargo::char
                    cargo_comissao_detalhes[5]    = "'"+cargo_comissao_detalhes[5]+"'"          if len(cargo_comissao_detalhes[5]) > 0 else "NULL"            # dataEntrada::date
                    cargo_comissao_detalhes[6]    = "'"+cargo_comissao_detalhes[6]+"'"          if len(cargo_comissao_detalhes[6]) > 0 else "NULL"            # dataSaida::date

                    sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes_comissoes_cargos ("
                        "capnum, datacaptura, "
                        "numlegislatura, idecadastro, "
                        "idorgaolegislativocd, siglacomissao, nomecomissao, "
                        "idcargo, nomecargo, dataentrada, datasaida "
                        ") VALUES ("
                        "{0}, '{1}', "
                        "{2}, {3}, "
                        "{4}, {5}, {6}, "
                        "{7}, {8}, {9}, {10} "
                        ")").format(nextcapnum, st, p[0], p[7], cargo_comissao_detalhes[0], cargo_comissao_detalhes[1], cargo_comissao_detalhes[2], cargo_comissao_detalhes[3], cargo_comissao_detalhes[4], cargo_comissao_detalhes[5], cargo_comissao_detalhes[6])
                    #print sql
                    cursor.execute(sql)


            ###################################
            # PERÍODOS EXERCÍCIO
            ###################################
            for periodos_exercicio in i.findall('periodosExercicio'):
                for periodo_exercicio in periodos_exercicio.findall('periodoExercicio'):
                    periodo_exercicio_detalhes = [periodo_exercicio.find(n).text for n in ('siglaUFRepresentacao', 'situacaoExercicio', 'dataInicio', 'dataFim', 'idCausaFimExercicio', 'descricaoCausaFimExercicio', 'idCadastroParlamentarAnterior')]
                    for x in range(len(periodo_exercicio_detalhes)):
                        if periodo_exercicio_detalhes[x]:
                            periodo_exercicio_detalhes[x] = periodo_exercicio_detalhes[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                        else:
                            periodo_exercicio_detalhes[x] = ''

                    #print periodo_exercicio_detalhes

                    # Arruma aspas e nulls
                    periodo_exercicio_detalhes[0]    = "'"+periodo_exercicio_detalhes[0]+"'"          if len(periodo_exercicio_detalhes[0]) > 0 else "NULL"            # siglaUFRepresentacao::char
                    periodo_exercicio_detalhes[1]    = "'"+periodo_exercicio_detalhes[1]+"'"          if len(periodo_exercicio_detalhes[1]) > 0 else "NULL"            # situacaoExercicio::char
                    periodo_exercicio_detalhes[2]    = "'"+periodo_exercicio_detalhes[2]+"'"          if len(periodo_exercicio_detalhes[2]) > 0 else "NULL"            # dataInicio::date
                    periodo_exercicio_detalhes[3]    = "'"+periodo_exercicio_detalhes[3]+"'"          if len(periodo_exercicio_detalhes[3]) > 0 else "NULL"            # dataFim::date
                    periodo_exercicio_detalhes[4]    =     periodo_exercicio_detalhes[4]              if len(periodo_exercicio_detalhes[4]) > 0 else "NULL"            # idCausaFimExercicio::int
                    periodo_exercicio_detalhes[5]    = "'"+periodo_exercicio_detalhes[5]+"'"          if len(periodo_exercicio_detalhes[5]) > 0 else "NULL"            # descricaoCausaFimExercicio::char
                    periodo_exercicio_detalhes[6]    =     periodo_exercicio_detalhes[6]              if len(periodo_exercicio_detalhes[6]) > 0 else "NULL"            # idCadastroParlamentarAnterior::int

                    sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes_periodos_exercicio ("
                        "capnum, datacaptura, "
                        "numlegislatura, idecadastro, "
                        "siglaUFRepresentacao, situacaoExercicio, dataInicio, dataFim, "
                        "idCausaFimExercicio, descricaoCausaFimExercicio, idCadastroParlamentarAnterior "
                        ") VALUES ("
                        "{0}, '{1}', "
                        "{2}, {3}, "
                        "{4}, {5}, {6}, "
                        "{7}, {8}, {9}, {10} "
                        ")").format(nextcapnum, st, p[0], p[7], periodo_exercicio_detalhes[0], periodo_exercicio_detalhes[1], periodo_exercicio_detalhes[2], periodo_exercicio_detalhes[3], periodo_exercicio_detalhes[4], periodo_exercicio_detalhes[5], periodo_exercicio_detalhes[6])
                    #print sql
                    cursor.execute(sql)


            ###################################
            # FILIAÇÕES PARTIDÁRIAS
            ###################################
            for filiacoes_partidarias in i.findall('filiacoesPartidarias'):
                for filiacao_partidarias in filiacoes_partidarias.findall('filiacaoPartidaria'):
                    filiacao_partidarias_detalhes = [filiacao_partidarias.find(n).text for n in ('idPartidoAnterior', 'siglaPartidoAnterior', 'nomePartidoAnterior', 'idPartidoPosterior', 'siglaPartidoPosterior', 'nomePartidoPosterior', 'dataFiliacaoPartidoPosterior')]
                    for x in range(len(filiacao_partidarias_detalhes)):
                        if filiacao_partidarias_detalhes[x]:
                            filiacao_partidarias_detalhes[x] = filiacao_partidarias_detalhes[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                        else:
                            filiacao_partidarias_detalhes[x] = ''

                    #print filiacao_partidarias_detalhes

                    # Arruma aspas e nulls
                    filiacao_partidarias_detalhes[0]    = "'"+filiacao_partidarias_detalhes[0]+"'"          if len(filiacao_partidarias_detalhes[0]) > 0 else "NULL"            # idPartidoAnterior::char
                    filiacao_partidarias_detalhes[1]    = "'"+filiacao_partidarias_detalhes[1]+"'"          if len(filiacao_partidarias_detalhes[1]) > 0 else "NULL"            # siglaPartidoAnterior::char
                    filiacao_partidarias_detalhes[2]    = "'"+filiacao_partidarias_detalhes[2]+"'"          if len(filiacao_partidarias_detalhes[2]) > 0 else "NULL"            # nomePartidoAnterior::char
                    filiacao_partidarias_detalhes[3]    = "'"+filiacao_partidarias_detalhes[3]+"'"          if len(filiacao_partidarias_detalhes[3]) > 0 else "NULL"            # idPartidoPosterior::char
                    filiacao_partidarias_detalhes[4]    = "'"+filiacao_partidarias_detalhes[4]+"'"          if len(filiacao_partidarias_detalhes[4]) > 0 else "NULL"            # siglaPartidoPosterior::char
                    filiacao_partidarias_detalhes[5]    = "'"+filiacao_partidarias_detalhes[5]+"'"          if len(filiacao_partidarias_detalhes[5]) > 0 else "NULL"            # nomePartidoPosterior::char
                    filiacao_partidarias_detalhes[6]    = "'"+filiacao_partidarias_detalhes[6]+"'"          if len(filiacao_partidarias_detalhes[6]) > 0 else "NULL"            # dataFiliacaoPartidoPosterior::date

                    sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes_filiacoes ("
                        "capnum, datacaptura, "
                        "numlegislatura, idecadastro, "
                        "idPartidoAnterior, siglaPartidoAnterior, nomePartidoAnterior, "
                        "idPartidoPosterior, siglaPartidoPosterior, nomePartidoPosterior, dataFiliacaoPartidoPosterior "
                        ") VALUES ("
                        "{0}, '{1}', "
                        "{2}, {3}, "
                        "{4}, {5}, {6}, "
                        "{7}, {8}, {9}, {10} "
                        ")").format(nextcapnum, st, p[0], p[7], filiacao_partidarias_detalhes[0], filiacao_partidarias_detalhes[1], filiacao_partidarias_detalhes[2], filiacao_partidarias_detalhes[3], filiacao_partidarias_detalhes[4], filiacao_partidarias_detalhes[5], filiacao_partidarias_detalhes[6])
                    #print sql
                    cursor.execute(sql)


            ###################################
            # HISTÓRICO LIDERANÇA
            ###################################
            for historico_lider in i.findall('historicoLider'):
                for historico_lider_item in historico_lider.findall('itemHistoricoLider'):
                    historico_lider_item_detalhes = [historico_lider_item.find(n).text for n in ('idHistoricoLider', 'idCargoLideranca', 'descricaoCargoLideranca', 'numOrdemCargo', 'dataDesignacao', 'dataTermino', 'codigoUnidadeLideranca', 'siglaUnidadeLideranca', 'idBlocoPartido')]
                    for x in range(len(historico_lider_item_detalhes)):
                        if historico_lider_item_detalhes[x]:
                            historico_lider_item_detalhes[x] = historico_lider_item_detalhes[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                        else:
                            historico_lider_item_detalhes[x] = ''

                    #print historico_lider_item_detalhes

                    # Arruma aspas e nulls
                    historico_lider_item_detalhes[0]    =     historico_lider_item_detalhes[0]              if len(historico_lider_item_detalhes[0]) > 0 else "NULL"            # idHistoricoLider::int
                    historico_lider_item_detalhes[1]    = "'"+historico_lider_item_detalhes[1]+"'"          if len(historico_lider_item_detalhes[1]) > 0 else "NULL"            # idCargoLideranca::char
                    historico_lider_item_detalhes[2]    = "'"+historico_lider_item_detalhes[2]+"'"          if len(historico_lider_item_detalhes[2]) > 0 else "NULL"            # descricaoCargoLideranca::char
                    historico_lider_item_detalhes[3]    =     historico_lider_item_detalhes[3]              if len(historico_lider_item_detalhes[3]) > 0 else "NULL"            # numOrdemCargo::int
                    historico_lider_item_detalhes[4]    = "'"+historico_lider_item_detalhes[4]+"'"          if len(historico_lider_item_detalhes[4]) > 0 else "NULL"            # dataDesignacao::date
                    historico_lider_item_detalhes[5]    = "'"+historico_lider_item_detalhes[5]+"'"          if len(historico_lider_item_detalhes[5]) > 0 else "NULL"            # dataTermino::date
                    historico_lider_item_detalhes[6]    = "'"+historico_lider_item_detalhes[6]+"'"          if len(historico_lider_item_detalhes[6]) > 0 else "NULL"            # codigoUnidadeLideranca::char
                    historico_lider_item_detalhes[7]    = "'"+historico_lider_item_detalhes[7]+"'"          if len(historico_lider_item_detalhes[7]) > 0 else "NULL"            # siglaUnidadeLideranca::char
                    historico_lider_item_detalhes[8]    = "'"+historico_lider_item_detalhes[8]+"'"          if len(historico_lider_item_detalhes[8]) > 0 else "NULL"            # idBlocoPartido::char

                    sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes_lideranca ("
                        "capnum, datacaptura, "
                        "numlegislatura, idecadastro, "
                        "idHistoricoLider, idCargoLideranca, descricaoCargoLideranca, numOrdemCargo, "
                        "dataDesignacao, dataTermino, codigoUnidadeLideranca, siglaUnidadeLideranca, idBlocoPartido "
                        ") VALUES ("
                        "{0}, '{1}', "
                        "{2}, {3}, "
                        "{4}, {5}, {6}, "
                        "{7}, {8}, {9}, {10}, {11}, {12} "
                        ")").format(nextcapnum, st, p[0], p[7], historico_lider_item_detalhes[0], historico_lider_item_detalhes[1], historico_lider_item_detalhes[2], historico_lider_item_detalhes[3], historico_lider_item_detalhes[4], historico_lider_item_detalhes[5], historico_lider_item_detalhes[6], historico_lider_item_detalhes[7], historico_lider_item_detalhes[8])
                    #print sql
                    cursor.execute(sql)


            # Arruma Encode
            for x in range(len(p)):
                if p[x]:
                    p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                else:
                    p[x] = ''

            # Arruma aspas e nulls
            p[0]    =     p[0]              if len(p[0]) > 0 else "NULL"            # numlegislatura::int
            p[1]    = "'"+p[1]+"'"          if len(p[1]) > 0 else "NULL"            # email::char
            p[2]    = "'"+p[2]+"'"          if len(p[2]) > 0 else "NULL"            # nomeprofissao::char
            p[3]    = "'"+p[3]+"'"          if len(p[3]) > 0 else "NULL"            # datanascimento::date
            p[4]    = "'"+p[4]+"'"          if len(p[4]) > 0 else "NULL"            # datafalecimento::date
            p[5]    = "'"+p[5]+"'"          if len(p[5]) > 0 else "NULL"            # ufrepresentacaoatual::char
            p[6]    = "'"+p[6]+"'"          if len(p[6]) > 0 else "NULL"            # situacaonalegislaturaatual::char
            p[7]    =     p[7]              if len(p[7]) > 0 else "NULL"            # idecadastro::int
            p[8]    =     p[8]              if len(p[8]) > 0 else "NULL"            # idparlamentardeprecated::int
            p[9]    = "'"+p[9]+"'"          if len(p[9]) > 0 else "NULL"            # nomeparlamentaratual::char
            p[10]   = "'"+p[10]+"'"         if len(p[10]) > 0 else "NULL"            # nomecivil::char
            p[11]   = "'"+p[11]+"'"         if len(p[11]) > 0 else "NULL"            # sexo::char
            p[12]   = "'"+p[12]+"'"         if len(p[12]) > 0 else "NULL"            # partidoatualid::char
            p[13]   = "'"+p[13]+"'"         if len(p[13]) > 0 else "NULL"            # partidoatualsigla::char
            p[14]   = "'"+p[14]+"'"         if len(p[14]) > 0 else "NULL"            # partidoatualnome::char
            p[15]   = "'"+p[15]+"'"         if len(p[15]) > 0 else "NULL"            # gabinetenumero::char
            p[16]   = "'"+p[16]+"'"         if len(p[16]) > 0 else "NULL"            # gabineteanexo::char
            p[17]   = "'"+p[17]+"'"         if len(p[17]) > 0 else "NULL"            # gabinetetelefone::char

            #print p
            sql = ("INSERT INTO c_camdep.camdep_deputados_detalhes ("
                        "capnum, datacaptura, numlegislatura, email, nomeprofissao, "
                        "datanascimento, datafalecimento, ufrepresentacaoatual, "
                        "situacaonalegislaturaatual, idecadastro, idparlamentardeprecated, "
                        "nomeparlamentaratual, nomecivil, sexo, partidoatualid, "
                        "partidoatualsigla, partidoatualnome, gabinetenumero, "
                        "gabineteanexo, gabinetetelefone) VALUES ("
                        "{0}, '{1}', {2}, {3}, {4}, "
                        "{5}, {6}, {7}, "
                        "{8}, {9}, {10}, "
                        "{11}, {12}, {13}, {14}, "
                        "{15}, {16}, {17}, "
                        "{18}, {19})").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17])
            #print sql
            cursor.execute(sql)
            conn.commit()
            print 'Inserido deputado: {0}'.format(p[9])
            counter = counter+1
            if (counter % 10==0):
                print 'Inserts: {0}'.format(counter)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida captura de detalhes dos deputados"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
