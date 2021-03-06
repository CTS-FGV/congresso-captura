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

    # Connect
    conn = connect()
    cursor = conn.cursor()

    sql = "set datestyle to SQL,DMY;"
    cursor.execute(sql)

    # LOG
    cursor.execute('SELECT MAX(capnum) FROM c_camdep.camdep_capture_log')
    maxcapnum = cursor.fetchall()
    nextcapnum = maxcapnum[0][0]+1
    script = "cap_camdep_proposicao_sigla.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # Inicia captura de XML
    xml = urllib2.urlopen('http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ListarSiglasTipoProposicao')
    parser = etree.parse(xml)

    counter = 0
    for i in parser.findall('sigla'):

        p = []
        for attr, value in i.items():
            p.extend([value])
        #print p

        # Arruma Encode
        for x in range(len(p)):
            if p[x]:
                p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").strip()
            else:
                p[x] = ''

        # Arruma aspas e nulls
        p[0]    = "'"+p[0]+"'"          if len(p[0]) > 0 else "NULL"            # tipoSigla::char
        p[1]    = "'"+p[1]+"'"          if len(p[1]) > 0 else "NULL"            # descricao::char
        p[2]    =     p[2]              if len(p[2]) > 0 else "NULL"            # ativa::bool
        p[3]    = "'"+p[3]+"'"          if len(p[3]) > 0 else "NULL"            # genero::char

        #print p
        sql = ("INSERT INTO c_camdep.camdep_proposicoes_siglas ("
                    "capnum, datacaptura, tiposigla, descricao, ativa, genero"
                    ") VALUES ("
                    "{0}, '{1}', {2}, {3}, {4}, {5}"
                    ")").format(nextcapnum, st, p[0], p[1], p[2], p[3])
        #print sql
        cursor.execute(sql)
        conn.commit()
        counter = counter+1
        if (counter % 10==0):
            print 'Inserts: {0}'.format(counter)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida captura de siglas de proposições"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
