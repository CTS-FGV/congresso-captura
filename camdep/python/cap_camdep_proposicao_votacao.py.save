#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
# DESCRIÇÃO:
# Captura os votos das proposições levadas a plenário (resultado, orientação bancada e votos dos deputados).
#
# TABELAS POPULADAS:
# - c_camdep.camdep_proposicoes_votacao
# - c_camdep.camdep_proposicoes_votacao_bancada
# - c_camdep.camdep_proposicoes_votacao_deputado
#
# CRON:
# Deve ser executada toda noite, para pegar as votações até o dia anterior.
#
# WEBSERVICE:
# http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/obtervotacaoproposicao
#
# TODO:
# 1. Não está incremental (só pegar votações de proposições novas).
# OBS IMPORTANTE: acho que está resolvido.
#
# 2. Está com um erro para pegar proposições que tenham tido seu nome alterado com "=>"
# NÃO ESTÁ PRONTO PARA SER RODADO TODAS AS NOITES.
####################################################################################################


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
    script = "cap_camdep_proposicao_votacao.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # LISTA PROPOSICOES A SEREM CAPTURADAS
    cursor.execute('''
            SELECT DISTINCT codproposicao, tipo, numero, ano
            FROM (
                SELECT DISTINCT
                    a.codproposicao,
                    a.datavotacao,
                    split_part(a.nomeproposicao, ' ', 1) tipo,
                    split_part(split_part(a.nomeproposicao, '/', 1),' ',2) numero,
                    split_part(split_part(a.nomeproposicao, '/', 2),' ',1) ano
                FROM c_camdep.camdep_proposicoes_plenario_ano a
                LEFT OUTER JOIN c_camdep.camdep_proposicoes_votacao v
                ON a.codproposicao = v.codproposicao
                    AND a.datavotacao::date = v.datavotacao::date
                WHERE v.id IS NULL
            ) tmp
            ORDER BY tipo, numero DESC
            '''
        )
    proposicoes = cursor.fetchall()

    counter = 0
    loop_main = 1

    for proposicao in proposicoes:

        # Inicia captura de XML
        url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterVotacaoProposicao?tipo={0}&numero={1}&ano={2}'.format(proposicao[1], proposicao[2], proposicao[3])
        #print url

        try:
            print '{0}/{1} - {2}'.format(loop_main, len(proposicoes), url)
            xml = urllib2.urlopen(url)
            tree = etree.parse(xml)
            root = tree.getroot()

            for child in root:
                for child2 in child:
                    p = []
                    p.extend([str(proposicao[0])]) #codproposicao
                    p.extend([str(proposicao[1])]) #tipo
                    p.extend([str(proposicao[2])]) #numero
                    p.extend([str(proposicao[3])]) #ano
                    p.extend([child2.attrib['codSessao']])
                    p.extend([child2.attrib['Data'] +' '+ child2.attrib['Hora']+':00'])
                    p.extend([child2.attrib['Resumo']])
                    p.extend([child2.attrib['ObjVotacao']])
                    # Arruma Encode
                    for x in range(len(p)):
                        if p[x]:
                            p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").replace("\n"," ").strip()
                        else:
                            p[x] = ''

                    # Arruma aspas e nulls
                    p[0]     =     p[0]               if len(p[0]) > 0 else "NULL"            # codProposicao::int
                    p[1]     = "'"+p[1]+"'"           if len(p[1]) > 0 else "NULL"            # tipo::char
                    p[2]     =     p[2]               if len(p[2]) > 0 else "NULL"            # numero::int
                    p[3]     =     p[3]               if len(p[3]) > 0 else "NULL"            # ano::int
                    p[4]     =     p[4]               if len(p[4]) > 0 else "NULL"            # codSessao::int
                    p[5]     = "'"+p[5]+"'"           if len(p[5]) > 0 else "NULL"            # dataVotacao::date
                    p[6]     = "'"+p[6]+"'"           if len(p[6]) > 0 else "NULL"            # Resumo::char
                    p[7]     = "'"+p[7]+"'"           if len(p[7]) > 0 else "NULL"            # objVotacao::char

                    sql = ("INSERT INTO c_camdep.camdep_proposicoes_votacao ("
                                "capnum, datacaptura, codProposicao, tipo, numero, ano, codSessao, dataVotacao, Resumo, objVotacao"
                                ") VALUES ("
                                "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}"
                                ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7])
                    #print sql
                    try:
                        cursor.execute(sql)
                    except:
                        print sql
                        conn.close()
                        exit()

                    for child3 in child2:

                        for child4 in child3:
                            p = []
                            p.extend([str(proposicao[0])]) #codproposicao
                            p.extend([str(proposicao[1])]) #tipo
                            p.extend([str(proposicao[2])]) #numero
                            p.extend([str(proposicao[3])]) #ano
                            p.extend([child2.attrib['codSessao']])
                            p.extend([child2.attrib['Data'] +' '+ child2.attrib['Hora']+':00'])

                            if child3.tag == 'orientacaoBancada':
                                p.extend([child4.attrib['Sigla']])
                                p.extend([child4.attrib['orientacao']])
                                # Arruma Encode
                                for x in range(len(p)):
                                    if p[x]:
                                        p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").replace("\n"," ").strip()
                                    else:
                                        p[x] = ''

                                # Arruma aspas e nulls
                                p[0]     =     p[0]               if len(p[0]) > 0 else "NULL"            # codProposicao::int
                                p[1]     = "'"+p[1]+"'"           if len(p[1]) > 0 else "NULL"            # tipo::char
                                p[2]     =     p[2]               if len(p[2]) > 0 else "NULL"            # numero::int
                                p[3]     =     p[3]               if len(p[3]) > 0 else "NULL"            # ano::int
                                p[4]     =     p[4]               if len(p[4]) > 0 else "NULL"            # codSessao::int
                                p[5]     = "'"+p[5]+"'"           if len(p[5]) > 0 else "NULL"            # dataVotacao::date
                                p[6]     = "'"+p[6]+"'"           if len(p[6]) > 0 else "NULL"            # Sigla::char
                                p[7]     = "'"+p[7]+"'"           if len(p[7]) > 0 else "NULL"            # orientacao::char

                                sql = ("INSERT INTO c_camdep.camdep_proposicoes_votacao_bancada ("
                                            "capnum, datacaptura, codProposicao, tipo, numero, ano, codSessao, dataVotacao, Sigla, orientacao"
                                            ") VALUES ("
                                            "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}"
                                            ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7])
                                #print sql
                                cursor.execute(sql)


                            if child3.tag == 'votos':
                                p.extend([child4.attrib['ideCadastro']])
                                p.extend([child4.attrib['Nome']])
                                p.extend([child4.attrib['Partido']])
                                p.extend([child4.attrib['UF']])
                                p.extend([child4.attrib['Voto']])
                                # Arruma Encode
                                for x in range(len(p)):
                                    if p[x]:
                                        p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").replace("\n"," ").strip()
                                    else:
                                        p[x] = ''

                                # Arruma aspas e nulls
                                p[0]     =     p[0]               if len(p[0]) > 0 else "NULL"            # codProposicao::int
                                p[1]     = "'"+p[1]+"'"           if len(p[1]) > 0 else "NULL"            # tipo::char
                                p[2]     =     p[2]               if len(p[2]) > 0 else "NULL"            # numero::int
                                p[3]     =     p[3]               if len(p[3]) > 0 else "NULL"            # ano::int
                                p[4]     =     p[4]               if len(p[4]) > 0 else "NULL"            # codSessao::int
                                p[5]     = "'"+p[5]+"'"           if len(p[5]) > 0 else "NULL"            # dataVotacao::date
                                p[6]     =     p[6]               if len(p[6]) > 0 else "NULL"            # ideCadastro::int
                                p[7]     = "'"+p[7]+"'"           if len(p[7]) > 0 else "NULL"            # Nome::char
                                p[8]     = "'"+p[8]+"'"           if len(p[8]) > 0 else "NULL"            # Partido::char
                                p[9]     = "'"+p[9]+"'"           if len(p[9]) > 0 else "NULL"            # UF::char
                                p[10]    = "'"+p[10]+"'"          if len(p[10]) > 0 else "NULL"           # Voto::char

                                sql = ("INSERT INTO c_camdep.camdep_proposicoes_votacao_deputado ("
                                            "capnum, datacaptura, codProposicao, tipo, numero, ano, codSessao, dataVotacao, ideCadastro, Nome, Partido, UF, Voto"
                                            ") VALUES ("
                                            "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}"
                                            ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10])
                                #print sql
                                cursor.execute(sql)

                            counter += 1

            loop_main += 1
            conn.commit()

        except urllib2.HTTPError:
            print 'Não localizada'
            pass

    cursor.execute('''
        DELETE FROM c_camdep.camdep_proposicoes_votacao t1
        USING c_camdep.camdep_proposicoes_votacao t2
        WHERE
            t1.codproposicao = t2.codproposicao
            AND t1.tipo = t2.tipo
            AND t1.numero = t2.numero
            AND t1.ano = t2.ano
            AND t1.codsessao = t2.codsessao
            AND t1.datavotacao = t2.datavotacao
            AND t1.id > t2.id
        '''
    )

    cursor.execute('''
        DELETE FROM c_camdep.camdep_proposicoes_votacao_bancada t1
        USING c_camdep.camdep_proposicoes_votacao_bancada t2
        WHERE
            t1.codproposicao = t2.codproposicao
            AND t1.tipo = t2.tipo
            AND t1.numero = t2.numero
            AND t1.ano = t2.ano
            AND t1.codsessao = t2.codsessao
            AND t1.datavotacao = t2.datavotacao
            AND t1.sigla = t2.sigla
            AND t1.orientacao = t2.orientacao
            AND t1.id > t2.id
        '''
    )

    cursor.execute('''
        DELETE FROM c_camdep.camdep_proposicoes_votacao_deputado t1
        USING c_camdep.camdep_proposicoes_votacao_deputado t2
        WHERE
            t1.codproposicao = t2.codproposicao
            AND t1.tipo = t2.tipo
            AND t1.numero = t2.numero
            AND t1.ano = t2.ano
            AND t1.codsessao = t2.codsessao
            AND t1.datavotacao = t2.datavotacao
            AND t1.nome = t2.nome
            AND t1.id > t2.id
        '''
    )

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
