#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
# Gera índice de fidelidade governamental de partidos por semana
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
    script = "r_camdep_fid_gov_part_semana.py"

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Início da execução"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))
    conn.commit()


    ###########################################
    # LIMPANDO DADOS DA TABELA
    ###########################################
    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Limpando dados da tabela r_camdep.fid_gov_part_semana"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))
    conn.commit()

    # EXECUÇÃO
    cursor.execute("DELETE FROM r_camdep.fid_gov_part_semana")

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluída limpeza da tabela r_camdep.fid_gov_part_semana"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))
    conn.commit()

    ###########################################
    # INSERINDO DADOS DE FIDELIDADE
    ###########################################
    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Inserindo dados na r_camdep.fid_gov_part_semana"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))
    conn.commit()

    # EXECUÇÃO
    sql = """
    INSERT INTO r_camdep.fid_gov_part_semana
    SELECT
        CONCAT(EXTRACT(YEAR FROM d.datavotacao)::TEXT,'-',LPAD(EXTRACT(MONTH FROM d.datavotacao)::TEXT,2,'0'),'-',LPAD(EXTRACT(WEEK FROM d.datavotacao)::TEXT,2,'0')) anomessemana,
        d.partido,
        SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END) votos_governo,
        SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END) votos_contra,
        SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)+SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END) votos_total,
        100*(SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)/(SUM(CASE WHEN b.orientacao = d.voto THEN 1 ELSE 0 END)+SUM(CASE WHEN b.orientacao <> d.voto THEN 1 ELSE 0 END))::DECIMAL(10,4))::DECIMAL(10,4) percent_governo
    FROM
        c_camdep.camdep_proposicoes_votacao_bancada b,
        c_camdep.camdep_proposicoes_votacao_deputado d
    WHERE
        b.codproposicao = d.codproposicao
        AND b.datavotacao = d.datavotacao
        AND b.sigla = 'GOV.'
    GROUP BY
        1,2
    ORDER BY
        1,6 DESC
    """
    cursor.execute(sql)

    # LOG
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    detalhes = "Concluída inserção de dados na tabela r_camdep.fid_gov_part_semana"
    cursor.execute("INSERT INTO c_camdep.camdep_capture_log (capnum, script, datahora, detalhes) VALUES ({0}, '{1}', '{2}', '{3}')".format(nextcapnum,script,st,detalhes))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
