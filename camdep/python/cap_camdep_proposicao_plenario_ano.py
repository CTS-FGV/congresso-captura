#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
# DESCRIÇÃO:
# Captura todas as proposições votadas em plenário por ano.
#
# CRON:
# Deve ser executada toda noite, para pegar as proposições levadas a plenário até o dia anterior.
#
# WEBSERVICE:
# http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/ProposicoesVotadasEmPlenario
#
# TODO:
# 100% concluída.
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
    script = "cap_camdep_proposicao_plenario_ano.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # PEGA DATA ÚLTIMA CAPTURA
    cursor.execute('SELECT EXTRACT(YEAR FROM MAX(dataVotacao)) FROM c_camdep.camdep_proposicoes_plenario_ano')
    maxdatatramitacao = cursor.fetchall()
    maxdatatramitacao = int(maxdatatramitacao[0][0])
    #print maxdatatramitacao
    if maxdatatramitacao is None:
        maxdatatramitacao = 1991

    # DELETA REGISTROS PARCIAIS DO ÚLTIMO ANO
    cursor.execute("DELETE FROM c_camdep.camdep_proposicoes_plenario_ano WHERE EXTRACT(YEAR FROM dataVotacao) >= {0}".format(maxdatatramitacao))
    #print "DELETE FROM c_camdep.camdep_proposicoes_plenario_ano WHERE EXTRACT(YEAR FROM dataVotacao) >= '{0}'".format(maxdatatramitacao)
    #conn.close()
    #exit()

    counter = 0

    while maxdatatramitacao <= datetime.date.today().year:

        # Inicia captura de XML
        url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesVotadasEmPlenario?ano={0}&tipo='.format(maxdatatramitacao)
        print url

        try:
            xml = urllib2.urlopen(url)
            parser = etree.parse(xml)

            for i in parser.findall('proposicao'):

                p = [i.find(n).text for n in ('codProposicao', 'nomeProposicao', 'dataVotacao')]

                # Arruma Encode
                for x in range(len(p)):
                    if p[x]:
                        p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").replace("\n"," ").strip()
                    else:
                        p[x] = ''

                #print p

                # Arruma aspas e nulls
                p[0]     =     p[0]               if len(p[0]) > 0 else "NULL"            # codProposicao::char
                p[1]     = "'"+p[1]+"'"           if len(p[1]) > 0 else "NULL"            # nomeProposicao::char
                p[2]     = "'"+p[2]+"'"           if len(p[2]) > 0 else "NULL"            # dataVotacao::date

                sql = ("INSERT INTO c_camdep.camdep_proposicoes_plenario_ano ("
                            "capnum, datacaptura, codProposicao, nomeProposicao, dataVotacao"
                            ") VALUES ("
                            "{0}, '{1}', {2}, {3}, {4}"
                            ")").format(nextcapnum, st, p[0], p[1], p[2])

                #print sql
                #for x in range(len(p)):
                #    print p[x], len(p[x])
                #try:
                cursor.execute(sql)
                #except:
                #    print sql

                counter = counter+1
                if (counter % 10==0):
                    print 'Inserts: {0}'.format(counter)
                    conn.commit()

        except urllib2.HTTPError:
            print 'Erro url'
            pass

        maxdatatramitacao = maxdatatramitacao+1

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
