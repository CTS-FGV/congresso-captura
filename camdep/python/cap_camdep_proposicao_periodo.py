#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
# DESCRIÇÃO:
# Captura todas as proposições tramitadas em um determinado período.
#
# CRON:
# Deve ser executada toda noite, para pegar as proposições tramitadas até o dia anterior.
#
# WEBSERVICE:
# http://www2.camara.leg.br/transparencia/dados-abertos/dados-abertos-legislativo/webservices/proposicoes-1/listarProposicoesTramitadasNoPeriodo
#
# TODO:
# Entender se essa tramitação é qualquer tramitação ou apenas tramitação ao plenário.
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
    script = "cap_camdep_proposicao_periodo.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # PEGA DATA ÚLTIMA CAPTURA
    cursor.execute('SELECT (MAX(dataTramitacao)::date)::text FROM c_camdep.camdep_proposicoes_periodo')
    maxdatatramitacao = cursor.fetchall()
    maxdatatramitacao = maxdatatramitacao[0][0]
    #print maxdatatramitacao
    if maxdatatramitacao is None:
        maxdatatramitacao = '31/12/1945'

    dataInicio  = datetime.datetime.strptime(maxdatatramitacao, "%d/%m/%Y")
    dataInicio  = dataInicio + datetime.timedelta(days=-1)
    dataFim     = dataInicio + datetime.timedelta(days=7)

    # DELETA REGISTROS PARCIAIS DO ÚLTIMO DIA
    cursor.execute("DELETE FROM c_camdep.camdep_proposicoes_periodo WHERE datatramitacao >= '{0}'".format(dataInicio.strftime('%d/%m/%Y')))
    #print "DELETE FROM c_camdep.camdep_proposicoes_periodo WHERE datatramitacao >= '{0}'".format(dataInicio.strftime('%d/%m/%Y'))
    #conn.close()
    #exit()

    counter = 0
    counter_geral = 0
    #while dataInicio.date() <= datetime.date(1946,10,31):
    while dataInicio.date() <= datetime.datetime.today().date():

        # Inicia captura de XML
        print 'Intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarProposicoesTramitadasNoPeriodo?dtInicio={0}&dtFim={1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        #print url

        # LOG
        st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        detalhes = 'Inserindo proposições do intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

        try:
            xml = urllib2.urlopen(url)
            parser = etree.parse(xml)

            for i in parser.findall('proposicao'):

                p = [i.find(n).text for n in ('codProposicao', 'tipoProposicao', 'numero',
                                              'ano', 'dataTramitacao', 'dataAlteracao'
                                              )]
                # Arruma Encode
                for x in range(len(p)):
                    if p[x]:
                        p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                    else:
                        p[x] = ''

                # Arruma aspas e nulls
                p[0]    =     p[0]              if len(p[0]) > 0 else "NULL"            # codProposicao::int
                p[1]    = "'"+p[1]+"'"          if len(p[1]) > 0 else "NULL"            # tipoProposicao::char
                p[2]    =     p[2]              if len(p[2]) > 0 else "NULL"            # numero::int
                p[3]    =     p[3]              if len(p[3]) > 0 else "NULL"            # ano::int
                p[4]    = "'"+p[4]+"'"          if len(p[4]) > 0 else "NULL"            # dataTramitacao::datetime
                p[5]    = "'"+p[5]+"'"          if len(p[5]) > 0 else "NULL"            # dataAlteracao::datetime

                sql = ("INSERT INTO c_camdep.camdep_proposicoes_periodo ("
                            "capnum, datacaptura, codProposicao, tipoProposicao, numero, ano, dataTramitacao, dataAlteracao"
                            ") VALUES ("
                            "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}"
                            ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5])

                cursor.execute(sql)
                counter = counter+1
                counter_geral = counter_geral+1
                if (counter % 10==0):
                    print 'Inserts: {0}'.format(counter)
                    conn.commit()

        except urllib2.HTTPError:
            pass

        st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        detalhes = 'Proposições inseridas no intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

        counter = 0
        dataInicio  = dataInicio + datetime.timedelta(days=8)
        dataFim     = dataInicio + datetime.timedelta(days=7)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = 'Concluida captura de proposições até a data: {0}'.format(dataFim.strftime('%d/%m/%Y'))
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter_geral))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
