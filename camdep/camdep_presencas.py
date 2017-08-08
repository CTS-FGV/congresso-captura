import datetime

import congresso
import pandas as pd
import xmltodict
import copy
from sys import argv

import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')

CASA      = 'camdep'
API_TYPE  = 'sessoesreunioes'
API_NAME  = 'presencasdia2'


SCHEMA    = 'c_camdep'
LOG_TABLE = 'c_camdep.camara_capture_log'
TABLE  = ('_').join([CASA, API_TYPE, API_NAME])
SCRIPT = ('_').join([CASA, API_NAME, API_TYPE]) + '.py'

ENTRY_STRUCTURE = dict.fromkeys(['idParlamentar',
                                 'nomeParlamentar',
                                 'siglaPartido',
                                 'siglaUF',
                                 'justificativa',
                                 'presencaExterna',
                                 'dataSessao',
                                 'tipoSessao',
                                 'numeroSessao',
                                 'frequencia',
                                 'idSessao',
                                 'idFrequencia'])

def atualizar(delta_day):


    dataFim    = datetime.datetime.today() - datetime.timedelta(days=delta_day)
    dataInicio = datetime.datetime.today()
    print(dataFim, dataInicio)

    def daterange(start_date, end_date):
        for n in range(int((start_date - end_date).days)):
            yield start_date - datetime.timedelta(n)


    errors = {}
    camara = congresso.Camara()

    for single_date in daterange(dataInicio, dataFim):
        print(single_date.strftime("%d/%m/%Y"))
        bulk = []
        try:

            xml = camara.sessoes.listar_presencas_dia(single_date.strftime("%d/%m/%Y"))['content']
            try:
                xml = xmltodict.parse(xml)['dia']['parlamentares']
            except TypeError:
                continue


            count = 0
            engine = utils.connect_sqlalchemy()
            for parlamentar in xml['parlamentar']:
                entry = ENTRY_STRUCTURE
                entry['idParlamentar']   = parlamentar['carteiraParlamentar']
                entry['nomeParlamentar'] = parlamentar['nomeParlamentar'].split('-')[0]
                entry['siglaPartido']    = parlamentar['siglaPartido']
                entry['siglaUF']         = parlamentar['siglaUF']
                entry['justificativa']   = parlamentar['justificativa']
                entry['presencaExterna'] = parlamentar['presencaExterna']

                if not isinstance(parlamentar['sessoesDia']['sessaoDia'], list):
                    sessoes = [parlamentar['sessoesDia']['sessaoDia']]
                else:
                    sessoes = parlamentar['sessoesDia']['sessaoDia']

                for sessao in sessoes:

                    entry['dataSessao']   = datetime.datetime.strptime(sessao['inicio'], '%d/%m/%Y %H:%M:%S')
                    descricao = sessao['descricao'].split(' ')
                    entry['tipoSessao']   = descricao[0]
                    for d in descricao:
                        try:
                            num_sessao = int(d)
                        except:
                            pass
                    entry['numeroSessao'] = num_sessao 
                    entry['frequencia']   = sessao['frequencia']
                    bulk.append(copy.deepcopy(entry))
                    count += 1

            df = pd.DataFrame(bulk)
            df['idSessao'] = df[['dataSessao', 'numeroSessao']].apply(lambda x:
                                                                          hash(tuple(x)),
                                                                          axis=1)
            df['idFrequencia'] = df[['idSessao', 'idParlamentar']].apply(lambda x:
                                                                          hash(tuple(x)),
                                                    	                   axis=1)
            print('Adding {} entries to SQL table from {}'.format(len(df), single_date.strftime("%d/%m/%Y")))
            df.to_sql(TABLE, con=engine, schema=SCHEMA, if_exists='append')
        

        except Exception as E:
            import pickle as p
            
            errors[single_date.strftime("%d/%m/%Y")] =  E
            p.dump(errors, open('/cron-jobs/captura/cn-database/cn_database/camdep_presenca_errors.p', 'wb'))


atualizar(int(argv[1]))

"""


    print('Connecting...')
    conn = utils.connect_psycopg2()
    cursor = conn.cursor()

    sql = "set datestyle to SQL,DMY;"
    cursor.execute(sql)
    print('Connected')

    # Get nextcapnum
    cursor.execute('SELECT MAX(capnum) FROM {}'.format(LOG_TABLE))
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
    
# LOG

        #utils.logging(script=SCRIPT, cursor=cursor, nextcapnum=nextcapnum, log_table=LOG_TABLE, quantidade=count,
        #              detalhes="Proposições inseridas no intervalo: {0} - {1}".format(
        #                  dataInicio.strftime('%d/%m/%Y'),
        #                  dataFim.strftime('%d/%m/%Y')))

    #conn.commit()
    #conn.close()
"""