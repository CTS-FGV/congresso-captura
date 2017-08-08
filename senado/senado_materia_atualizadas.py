import datetime
import time

import pandas as pd
import requests
import utils
import xmltodict

API_NAME  = 'atualizadas'
API_TYPE  = 'materia'
CASA      = 'senado'
SCHEMA    = 'c_senado'
LOG_TABLE = 'c_senado.senado_capture_log'
TABLE  = ('_').join([CASA, API_TYPE, API_NAME])
SCRIPT = ('_').join([CASA, API_NAME, API_TYPE]) + '.py'

def atualizar():
    """
    Gets a XML file and parses it to a .tsv file using the skeleton.

    Raises an error if XML is empty
    """
    print('Connecting...')
    conn = utils.connect_psycopg2()
    cursor = conn.cursor()

    sql = "set datestyle to SQL,DMY;"
    cursor.execute(sql)
    print('Connected')

    # Get nextcapnum
    cursor.execute('SELECT MAX(capnum) FROM c_senado.senado_capture_log')
    maxcapnum = cursor.fetchall()
    if maxcapnum[0][0] == None:
        nextcapnum = 1
    else:
         nextcapnum = maxcapnum[0][0] + 1

   # LOG INICIO EXECUCAO
    utils.logging(script=SCRIPT, cursor=cursor, nextcapnum=nextcapnum, log_table=LOG_TABLE,
                  detalhes="Início da execução")


    # PEGA DATA ÚLTIMA CAPTURA
    cursor.execute('SELECT (MAX({})::date)::text FROM {}'.format(('.').join([TABLE, 'DataUltimaAtualizacao']),
                                                                 ('.').join([SCHEMA,TABLE])))
    maxdatatramitacao = cursor.fetchall()
    maxdatatramitacao = maxdatatramitacao[0][0]
    if maxdatatramitacao is None:
        maxdatatramitacao = '31/12/1945'


    dataInicio = datetime.datetime.strptime(maxdatatramitacao, "%d/%m/%Y")
    dataInicio = dataInicio + datetime.timedelta(days=-1)
    dataFim = datetime.datetime.today()

    #DELETE PARTIAL
    cursor.execute("DELETE FROM {0}"
                   " WHERE {1} >= '{2}'".format(('.').join([SCHEMA,TABLE]),
                                                ('.').join([TABLE, 'DataUltimaAtualizacao']),
                                                dataInicio.strftime('%d/%m/%Y')))

    # LOG
    utils.logging(script=SCRIPT, cursor=cursor, nextcapnum=nextcapnum, log_table=LOG_TABLE,
                  detalhes="Inserindo proposições do intervalo {0} - {1}".format(dataInicio.strftime('%d/%m/%Y'),
                                                                                 dataFim.strftime('%d/%m/%Y')))
    # CAPTURA XML
    print('Intervalo: {0} - {1}'.format(dataInicio.strftime('%d/%m/%Y'), dataFim.strftime('%d/%m/%Y')))

    n_dias = (dataFim - dataInicio).days
    url = 'http://legis.senado.leg.br/dadosabertos/{}/{}?numdias={}'.format(API_TYPE, API_NAME, n_dias)
    xml =  requests.get(url).text
    xml = xmltodict.parse(xml)['ListaMateriasAtualizadas']['Materias']['Materia']

    # COMPLETA IDENTIFICACAO MATERIA

    engine = utils.connect_sqlalchemy()
    count = 0
    for child1 in xml:
        entry = {}
        entry['capnum'] = nextcapnum
        entry['datacaptura'] = datetime.datetime.fromtimestamp(time.time())
        entry['CodigoMateria'] = int(child1['IdentificacaoMateria']['CodigoMateria'])
        entry['NumeroMateria'] = child1['IdentificacaoMateria']['NumeroMateria']

        branch = child1['AtualizacoesRecentes']['Atualizacao']
        if not isinstance(branch, list):
            branch = [branch]
        for child2 in branch:
            entry['InformacaoAtualizada']  = child2['InformacaoAtualizada']
            entry['UrlServicoAfetado']     = child2['UrlServicoAfetado']
            entry['DataUltimaAtualizacao'] = datetime.datetime.strptime(child2['DataUltimaAtualizacao'], "%Y-%m-%d %H:%M:%S")

            df = pd.DataFrame(entry, index=[0])
            df.rename(columns=lambda x: x.lower(), inplace=True)
            print('Adding to SQL table', count)
            df.to_sql(TABLE, con=engine, schema=SCHEMA, if_exists='append')
            count += 1

    # LOG
    utils.logging(script=SCRIPT, cursor=cursor, nextcapnum=nextcapnum, log_table=LOG_TABLE, quantidade=count,
                  detalhes="Proposições inseridas no intervalo: {0} - {1}".format(
                      dataInicio.strftime('%d/%m/%Y'),
                      dataFim.strftime('%d/%m/%Y')))

    conn.commit()
    conn.close()

atualizar()
