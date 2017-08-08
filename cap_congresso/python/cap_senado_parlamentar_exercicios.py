from tqdm import tqdm
import pandas as pd
import copy
import datetime
import sys
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

class Parlamentar_Exercicios_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_parlamentar_exercicios(self, exercicio, entry_columns):
        tgd = utils.try_get_data

        id_api = exercicio['CodigoExercicio']
        api_captura = "SF"
        id_exercicio = utils.generate_hash(id_api,api_captura)

        entry = copy.deepcopy(entry_columns)
        entry['id_exercicio'] = id_exercicio
        entry['id_api'] = id_api
        entry['sigla_casa'] = 'SF'
        entry['api_captura'] = api_captura
        entry['data_captura'] = datetime.datetime.today()
        entry['causa_fim_exercicio'] = tgd(exercicio,'DescricaoCausaAfastamento')
        data_inicio = tgd(exercicio,'DataInicio','date')
        data_fim = tgd(exercicio,'DataFim','date')

        if data_inicio:
            entry['data_inicio'] = data_inicio
            query = self.conn.execute("SELECT id_legislatura FROM c_congresso.legislaturas WHERE data_inicio <= '{}' AND data_fim >= '{}'".format(data_inicio, data_inicio))
            id_legislatura_inicio = list(query)[0][0]
            entry['id_legislatura_inicio'] = id_legislatura_inicio

        if data_fim:
            entry['data_fim'] = data_fim
            query = self.conn.execute("SELECT id_legislatura FROM c_congresso.legislaturas WHERE data_inicio <= '{}' AND data_fim >= '{}'".format(data_fim, data_fim))
            id_legislatura_fim = list(query)[0][0]
            entry['id_legislatura_fim'] = id_legislatura_fim

        return entry

    def update_DB(self,iterable, entry_columns):
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_exercicio FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]

        if isinstance(iterable,dict):
            iterable = [iterable]

        for exercicio in tqdm(iterable):
            entry = self.fill_entry_parlamentar_exercicios(exercicio,entry_columns)
            id_exercicio = entry['id_exercicio']

            if id_exercicio not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_exercicio)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_exercicio', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_exercicio',
                     'id_api',
                     'id_parlamentar',
                     'data_inicio',
                     'id_legislatura_inicio',
                     'data_fim',
                     'id_legislatura_fim',
                     'causa_fim_exercicio',
                     'sigla_casa',
                     'data_captura',
                     'api_captura',
                     'url_captura']

    conn = utils.connect_sqlalchemy()
    list_of_parlamentares = conn.execute('SELECT id_parlamentar,id_senado FROM c_congresso.parlamentar_detalhe')

    for (id_parlamentar,id_senado) in tqdm(list_of_parlamentares):

        cap = Parlamentar_Exercicios_Senado(table='parlamentar_exercicios', schema='c_congresso', entry_columns=entry_columns)
        cap.entry_columns['id_parlamentar'] = id_parlamentar

        url = 'http://legis.senado.leg.br/dadosabertos/senador/{}/historico'.format(id_senado)
        cap.entry_columns['url_captura'] = url
        data_source = utils.get_json(url)

        try:
            iterable = data_source['DetalheParlamentar']['Parlamentar']['UltimoMandato']['Exercicios']['Exercicio']
            cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)
        except KeyError:
            try:
                iterable = data_source['DetalheParlamentar']['Parlamentar']['MandatoAtual']['Exercicios']['Exercicio']
                cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)
            except KeyError:
                if len(data_source['DetalheParlamentar']['Parlamentar']) > 4:
                    print(url)

