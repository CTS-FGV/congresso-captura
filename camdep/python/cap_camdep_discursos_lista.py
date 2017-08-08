#!/usr/bin/python
# -*- coding: utf-8 -*-

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

    # Initialize variables
    maxdata = None

    # Connect
    conn = connect()
    cursor = conn.cursor()

    sql = "set datestyle to SQL,DMY;"
    cursor.execute(sql)

    # LOG
    cursor.execute('SELECT MAX(capnum) FROM c_camdep.camdep_capture_log')
    maxcapnum = cursor.fetchall()
    nextcapnum = maxcapnum[0][0]+1
    script = "cap_camdep_discursos_lista.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # PEGA DATA ÚLTIMA CAPTURA
    cursor.execute('SELECT (MAX(data)::date)::text FROM c_camdep.camdep_discursos_lista')
    maxdata = cursor.fetchall()
    maxdata = maxdata[0][0]
    #print maxdata
    if maxdata is None:
        maxdata = '01/01/2001'

    dataInicio  = datetime.datetime.strptime(maxdata, "%d/%m/%Y")
    dataInicio  = dataInicio + datetime.timedelta(days=-1)
    dataFim     = dataInicio + datetime.timedelta(days=90)

    # DELETA REGISTROS PARCIAIS DO ÚLTIMO DIA
    cursor.execute("DELETE FROM c_camdep.camdep_discursos_lista WHERE data > '{0}'".format(dataInicio.strftime('%d/%m/%Y')))
    #print "DELETE FROM c_camdep.camdep_proposicoes_periodo WHERE datatramitacao >= '{0}'".format(dataInicio.strftime('%d/%m/%Y'))

    counter = 0
    #while dataInicio.date() <= datetime.date(2001,06,30):
    while dataInicio.date() <= datetime.datetime.today().date():

        # Inicia captura de XML
        print 'Intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        url = 'http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/ListarDiscursosPlenario?dataIni={0}&dataFim={1}&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF='.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        #print url

        # LOG
        st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        detalhes = 'Inserindo lista de discursos do intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'),dataFim.strftime('%d/%m/%Y'))
        cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

        try:
            xml = urllib2.urlopen(url)
            tree = etree.parse(xml)
            root = tree.getroot()

            for sessao in root:

                p_final = []

                p_sessao = [sessao.find(n).text for n in ('codigo', 'data', 'numero', 'tipo')]

                # Arruma Encode
                for x in range(len(p_sessao)):
                    if p_sessao[x]:
                        p_sessao[x] = p_sessao[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                    else:
                        p_sessao[x] = ''

                # Arruma aspas e nulls
                p_sessao[0]    = "'"+p_sessao[0]+"'"          if len(p_sessao[0]) > 0 else "NULL"            # codigo::char
                p_sessao[1]    = "'"+p_sessao[1]+"'"          if len(p_sessao[1]) > 0 else "NULL"            # data::date
                p_sessao[2]    =     p_sessao[2]              if len(p_sessao[2]) > 0 else "NULL"            # numero::int
                p_sessao[3]    = "'"+p_sessao[3]+"'"          if len(p_sessao[3]) > 0 else "NULL"            # tipo::char


                for fasesSessao in sessao:

                    for faseSessao in fasesSessao:

                        p_fasesessao = [faseSessao.find(n).text for n in ('codigo', 'descricao')]

                        # Arruma Encode
                        for x in range(len(p_fasesessao)):
                            if p_fasesessao[x]:
                                p_fasesessao[x] = p_fasesessao[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                            else:
                                p_fasesessao[x] = ''

                        # Arruma aspas e nulls
                        p_fasesessao[0]    = "'"+p_fasesessao[0]+"'"          if len(p_fasesessao[0]) > 0 else "NULL"            # codigo::char
                        p_fasesessao[1]    = "'"+p_fasesessao[1]+"'"          if len(p_fasesessao[1]) > 0 else "NULL"            # descricao::char


                        for discursos in faseSessao:

                            for discurso in discursos:

                                for orador in discurso.findall('orador'):

                                    p_orador = [orador.find(n).text for n in ('numero', 'nome', 'partido', 'uf')]

                                    # Arruma Encode
                                    for x in range(len(p_orador)):
                                        if p_orador[x]:
                                            p_orador[x] = p_orador[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                                        else:
                                            p_orador[x] = ''

                                    # Arruma aspas e nulls
                                    p_orador[0]    =     p_orador[0]              if len(p_orador[0]) > 0 else "NULL"            # numero::int
                                    p_orador[1]    = "'"+p_orador[1]+"'"          if len(p_orador[1]) > 0 else "NULL"            # nome::char
                                    p_orador[2]    = "'"+p_orador[2]+"'"          if len(p_orador[2]) > 0 else "NULL"            # partido::char
                                    p_orador[3]    = "'"+p_orador[3]+"'"          if len(p_orador[3]) > 0 else "NULL"            # uf::char


                                p_discurso = [discurso.find(n).text for n in ('horaInicioDiscurso', 'txtIndexacao', 'numeroQuarto', 'numeroInsercao', 'sumario')]

                                # Arruma Encode
                                for x in range(len(p_discurso)):
                                    if p_discurso[x]:
                                        p_discurso[x] = p_discurso[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
                                    else:
                                        p_discurso[x] = ''

                                # Arruma aspas e nulls
                                p_discurso[0]    = "'"+p_discurso[0]+"'"          if len(p_discurso[0]) > 0 else "NULL"            # horaInicioDiscurso::data
                                p_discurso[1]    = "'"+p_discurso[1]+"'"          if len(p_discurso[1]) > 0 else "NULL"            # txtIndexacao::char
                                p_discurso[2]    =     p_discurso[2]              if len(p_discurso[2]) > 0 else "NULL"            # numeroQuarto::int
                                p_discurso[3]    =     p_discurso[3]              if len(p_discurso[3]) > 0 else "NULL"            # numeroInsercao::int
                                p_discurso[4]    = "'"+p_discurso[4]+"'"          if len(p_discurso[4]) > 0 else "NULL"            # sumario::char


                                counter += 1
                                p_final.extend(p_sessao)
                                p_final.extend(p_fasesessao)
                                p_final.extend(p_orador)
                                p_final.extend(p_discurso)

                                p = p_final

                                sql = ("INSERT INTO c_camdep.camdep_discursos_lista ("
                                            "capnum, datacaptura, codigo, data, numero, tipo, fasecodigo, "
                                            "fasedescricao, fasenumero, nome, partido, uf, horainiciodiscurso, "
                                            "txtindexacao, numeroquarto, numeroinsercao, sumario "
                                            ") VALUES ("
                                            "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}"
                                            ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14])

                                cursor.execute(sql)

                                if (counter % 10==0):
                                    print 'Inserts: {0}'.format(counter)
                                    conn.commit()

                                p_final = []
                                p = []


        except urllib2.HTTPError:
            pass

        dataInicio  = dataInicio + datetime.timedelta(days=91)
        dataFim     = dataInicio + datetime.timedelta(days=90)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = 'Concluida captura de lista de discursos até a data: {0}'.format(dataFim.strftime('%d/%m/%Y'))
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
