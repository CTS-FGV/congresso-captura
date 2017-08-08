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
    script = "cap_camdep_proposicao_id_daily.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    print 'Pegando lista de proposições'
    # LISTA ID DE TODAS AS PROPOSIÇÕES
    cursor.execute("SELECT DISTINCT codproposicao "
                    "FROM c_camdep.camdep_proposicoes_periodo "
                    "WHERE dataalteracao >= (SELECT MAX(datacaptura) - interval '2 hours' FROM c_camdep.camdep_proposicoes_id)")
    codproposicoes = cursor.fetchall()
    print 'Concluida lista de proposições'

    counter = 0

    for codproposicao in codproposicoes:

        codproposicao = codproposicao[0]

        # Inicia captura de XML
        url = 'http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/ObterProposicaoPorID?IdProp={0}'.format(codproposicao)
        #print counter

        try:
            xml = urllib2.urlopen(url)
            parser = etree.parse(xml)

            p = [parser.find(n).text for n in ('nomeProposicao', 'idProposicao', 'idProposicaoPrincipal',
                                          'nomeProposicaoOrigem', 'tipoProposicao', 'tema',
                                          'Ementa', 'ExplicacaoEmenta', 'Autor', 'ideCadastro',
                                          'ufAutor', 'partidoAutor', 'DataApresentacao',
                                          'RegimeTramitacao', 'UltimoDespacho', 'Apreciacao',
                                          'Indexacao', 'Situacao','LinkInteiroTeor', 'apensadas'
                                          )]

            # Arruma Encode
            for x in range(len(p)):
                if p[x]:
                    p[x] = p[x].encode('utf-8', 'ignore').replace("'","''").replace("\n    ","").replace("\n"," ").strip()
                else:
                    p[x] = ''

            # Arruma aspas e nulls
            p[0]     = "'"+p[0]+"'"           if len(p[0]) > 0 else "NULL"            # nomeProposicao::char
            p[1]     =     p[1]               if len(p[1]) > 0 else "NULL"            # idProposicao::int
            p[2]     = "'"+p[2]+"'"           if len(p[2]) > 0 else "NULL"            # idProposicaoPrincipal::int
            p[3]     = "'"+p[3]+"'"           if len(p[3]) > 0 else "NULL"            # nomeProposicaoOrigem::char
            p[4]     = "'"+p[4]+"'"           if len(p[4]) > 0 else "NULL"            # tipoProposicao::char
            p[5]     = "'"+p[5]+"'"           if len(p[5]) > 0 else "NULL"            # tema::char
            p[6]     = "'"+p[6]+"'"           if len(p[6]) > 0 else "NULL"            # Ementa::char
            p[7]     = "'"+p[7]+"'"           if len(p[7]) > 0 else "NULL"            # ExplicacaoEmenta::char
            p[8]     = "'"+p[8]+"'"           if len(p[8]) > 0 else "NULL"            # Autor::char
            p[9]     =     p[9]               if len(p[9]) > 0 else "NULL"            # ideCadastro::int
            p[10]    = "'"+p[10]+"'"          if len(p[10]) > 0 else "NULL"            # ufAutor::char
            p[11]    = "'"+p[11]+"'"          if len(p[11]) > 0 else "NULL"            # partidoAutor::char
            p[12]    = "'"+p[12]+"'"          if len(p[12]) > 0 else "NULL"            # DataApresentacao::datetime
            p[13]    = "'"+p[13]+"'"          if len(p[13]) > 0 else "NULL"            # RegimeTramitacao::char
            p[14]    = "'"+p[14]+"'"          if len(p[14]) > 0 else "NULL"            # UltimoDespacho::char
            p[15]    = "'"+p[15]+"'"          if len(p[15]) > 0 else "NULL"            # Apreciacao::char
            p[16]    = "'"+p[16]+"'"          if len(p[16]) > 0 else "NULL"            # Indexacao::char
            p[17]    = "'"+p[17]+"'"          if len(p[17]) > 0 else "NULL"            # Situacao::datetime
            p[18]    = "'"+p[18]+"'"          if len(p[18]) > 0 else "NULL"            # LinkInteiroTeor::char
            p[19]    = "'"+p[19]+"'"          if len(p[19]) > 0 else "NULL"            # apensadas::char

            sql = ("INSERT INTO c_camdep.camdep_proposicoes_id ("
                        "capnum, datacaptura, nomeProposicao, idProposicao, idProposicaoPrincipal, nomeProposicaoOrigem, tipoProposicao, "
                        "tema, Ementa, ExplicacaoEmenta, Autor, ideCadastro, ufAutor, partidoAutor, DataApresentacao, RegimeTramitacao, "
                        "UltimoDespacho, Apreciacao, Indexacao, Situacao, LinkInteiroTeor, apensadas"
                        ") VALUES ("
                        "{0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}"
                        ")").format(nextcapnum, st, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19])

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

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluida execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes, quantidade) VALUES ({0}, '{1}', '{2}', '{3}', {4})".format(nextcapnum,script,st,detalhes,counter))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
