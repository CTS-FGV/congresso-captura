from tqdm import tqdm
import pandas as pd
import copy
import datetime
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

class Legislaturas_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_legislaturas(self, legislatura, entry_columns):
        tgd = utils.try_get_data

        id_api = legislatura['@id']
        sigla_casa = 'SF'
        id_legislatura = utils.generate_hash(id_api,sigla_casa)

        entry = copy.deepcopy(entry_columns)
        entry['id_legislatura'] = id_legislatura
        entry['id_api'] = int(id_api)
        entry['numero_legislatura'] = int(legislatura['NumeroLegislatura'])
        entry['sigla_casa'] = sigla_casa
        entry['data_inicio'] = tgd(legislatura,'DataInicio','date')
        entry['data_fim'] = tgd(legislatura,'DataFim','date')
        entry['data_eleicao'] = tgd(legislatura,'DataEleicao','date')
        entry['data_captura'] = datetime.datetime.today()
        entry['api_captura'] = "SF"

        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :return: None
        """
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_legislatura FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for legislatura in tqdm(iterable):

            entry = self.fill_entry_legislaturas(legislatura,entry_columns)
            id_legislatura = entry['id_legislatura']

            if id_legislatura not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_legislatura)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_legislatura', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_legislatura',
                     'id_api',
                     'sigla_casa',
                     'numero_legislatura',
                     'data_inicio',
                     'data_fim',
                     'data_eleicao',
                     'data_captura',
                     'url_captura']

    cap = Legislaturas_Senado(table='legislaturas', schema='c_congresso', entry_columns=entry_columns)
    url = 'http://legis.senado.leg.br/dadosabertos/plenario/lista/legislaturas'
    data_source = utils.get_json(url)
    iterable = data_source['ListaLegislatura']['Legislatura']['Legislatura']
    cap.entry_columns['url_captura'] = url
    cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)