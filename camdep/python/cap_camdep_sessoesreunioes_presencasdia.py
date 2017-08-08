import datetime

import congresso
import pandas as pd
import xmltodict
from sys import argv
import copy

import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')

CASA      = 'camdep'
API_TYPE  = 'sessoesreunioes'
API_NAME  = 'presencasdia2'


SCHEMA    = 'c_camdep'
LOG_TABLE = 'c_camdep.camara_capture_log'
TABLE  = ('_').join([CASA, API_TYPE, API_NAME])
SCRIPT = ('_').join([CASA, API_NAME, API_TYPE]) + '.py'

ENTRY_STRUCTURE = dict.fromkeys(['id_parlamentar',
                                 'nome_parlamentar',
                                 'sigla_partido',
                                 'sigla_uf',
                                 'justificativa',
                                 'presenca_externa',
                                 'data_sessao',
                                 'tipo_sessao',
                                 'numero_sessao',
                                 'frequencia',
                                 'id_sessao',
                                 'id_frequencia'])


def daterange(start_date, end_date):
    for n in range(int((start_date - end_date).days)):
        yield start_date - datetime.timedelta(n)

def atualizar(delta_day):

    conn = utils.connect_sqlalchemy()

    ## query vai de hoje para o último record
    if delta_day == 'production':
        dataFim    = pd.read_sql_query("SELECT max(datacaptura) FROM c_camdep.camdep_sessoesreunioes_presencasdia2",
                                       conn)
        dataFim = dataFim.iloc[0,0]
        dataInicio = datetime.datetime.today()

    elif delta_day == 'mount':
        dataFim = datetime.datetime.strptime('1999-01-01', '%Y-%m-%d')
        dataInicio = datetime.datetime.today()

    print(dataFim, dataInicio)

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
            for parlamentar in xml['parlamentar']:
                entry = copy.deepcopy(ENTRY_STRUCTURE)
                entry['id_parlamentar']   = parlamentar['carteiraParlamentar']
                entry['nome_parlamentar'] = parlamentar['nomeParlamentar'].split('-')[0]
                entry['sigla_partido']    = parlamentar['siglaPartido']
                entry['sigla_uf']         = parlamentar['siglaUF']
                entry['justificativa']    = parlamentar['justificativa']
                entry['presenca_externa'] = parlamentar['presencaExterna']

                if not isinstance(parlamentar['sessoesDia']['sessaoDia'], list):
                    sessoes = [parlamentar['sessoesDia']['sessaoDia']]
                else:
                    sessoes = parlamentar['sessoesDia']['sessaoDia']

                for sessao in sessoes:

                    entry['data_sessao']   = datetime.datetime.strptime(sessao['inicio'], '%d/%m/%Y %H:%M:%S')
                    descricao = sessao['descricao'].split(' ')
                    entry['tipo_sessao']   = descricao[0]
                    for d in descricao:
                        try:
                            num_sessao = int(d)
                        except:
                            pass
                    entry['numero_sessao'] = num_sessao
                    entry['frequencia']   = sessao['frequencia']
                    bulk.append(copy.deepcopy(entry))
                    count += 1

            df = pd.DataFrame(bulk)
            df['id_sessao'] = df[['data_sessao', 'numero_sessao']].apply(lambda x:
                                                                          hash(tuple(x)),
                                                                          axis=1)
            df['id_frequencia'] = df[['id_sessao', 'id_parlamentar']].apply(lambda x:
                                                                          hash(tuple(x)),
                                                    	                   axis=1)
            print('Adding {} entries to SQL table from {}'.format(len(df), single_date.strftime("%d/%m/%Y")))
            df.to_sql(TABLE, con=conn, schema=SCHEMA, if_exists='append')
        

        except Exception as E:
            import pickle as p
            
            errors[single_date.strftime("%d/%m/%Y")] =  E
            p.dump(errors, open('/cron-jobs/captura/cn-database/cn_database/camdep_presenca_errors.p', 'wb'))


atualizar(argv[1])

