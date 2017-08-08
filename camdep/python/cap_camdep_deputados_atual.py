#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
# É preciso inserir comentários no código.
#
#
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


    # LOG
    cursor.execute('SELECT MAX(capnum) FROM c_camdep.camdep_capture_log')
    maxcapnum = cursor.fetchall()
    nextcapnum = maxcapnum[0][0]+1
    script = "cap_camdep_deputados_atual.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Iniciando captura de XML: http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/ObterDeputados"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # Inicia captura de XML
    xml = urllib2.urlopen('http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/ObterDeputados')
    parser = etree.parse(xml)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida captura de XML: http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/ObterDeputados"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Iniciando parsing e salvando dados no banco"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    counter = 0
    for i in parser.findall('deputado'):

        p = [i.find(n).text for n in ('ideCadastro', 'condicao', 'matricula',
                                      'idParlamentar', 'nome', 'nomeParlamentar',
                                      'urlFoto', 'sexo', 'uf', 'partido', 'gabinete',
                                      'anexo', 'fone', 'email'
                                      )]

        for x in range(len(p)):
            if p[x]:
                p[x] = p[x].encode('utf-8', 'ignore').replace("'","''")
            else:
                p[x] = ''

        sql = "INSERT INTO c_camdep.camdep_deputados_atual (capnum, datacaptura, idecadastro, condicao, matricula, idparlamentar, nome, nomeparlamentar, urlfoto, sexo, uf, partido, gabinete, anexo, fone, email) VALUES ({0}, '{1}', {2}, '{3}', {4}, {5}, '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}')".format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13])

        print sql
        counter = counter+1
        cursor.execute(sql)


    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluído parsing e envio de dados para o banco"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
