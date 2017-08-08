from tqdm import tqdm
import pandas as pd
import copy
import datetime
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

class Partidos_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_partido(self, partido, entry_columns):
        tgd = utils.try_get_data

        entry = copy.deepcopy(entry_columns)
        entry['sigla'] = partido['Sigla']
        entry['nome'] = partido['Nome']
        entry['data_criacao'] = tgd(partido,'DataCriacao','date')
        entry['data_extincao'] = tgd(partido,'DataExtincao','date')
        entry['data_captura'] = datetime.datetime.today()
        entry['api_captura'] = 'SF'

        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :return:
        """
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT sigla FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for partido in tqdm(iterable):

            entry = self.fill_entry_partido(partido,entry_columns)
            sigla = entry['sigla']

            if sigla not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(sigla)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('sigla', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['sigla',
                     'nome',
                     'data_criacao',
                     'data_extincao',
                     'data_captura',
                     'url_captura']

    cap = Partidos_Senado(table='partidos', schema='c_congresso', entry_columns=entry_columns)
    url = 'http://legis.senado.leg.br/dadosabertos/senador/partidos?indAtivos=N'
    data_source = utils.get_json(url)
    iterable = data_source['ListaPartidos']['Partidos']['Partido']
    cap.entry_columns['url_captura'] = url
    cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)
