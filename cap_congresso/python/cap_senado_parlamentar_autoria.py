from tqdm import tqdm
import pandas as pd
import copy
import datetime
import sys
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

class Parlamentar_Autoria_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_parlamentar_autoria(self, autoria, entry_columns):
        tgd = utils.try_get_data

        IdentificacaoMateria = autoria['Materia']['IdentificacaoMateria']

        id_api = IdentificacaoMateria['CodigoMateria']
        api_captura = "SF"
        id_especie_legislativa = self.conn.execute("SELECT id_especie_legislativa FROM c_congresso.especies_legislativas WHERE id_api='{}' AND api_captura='{}'".format(id_api, api_captura))
        try:
            id_especie_legislativa = list(id_especie_legislativa)[0][0]
        except:
            id_especie_legislativa = ""

        entry = copy.deepcopy(entry_columns)
        entry['api_captura'] = api_captura
        entry['data_captura'] = datetime.datetime.today()
        entry['id_especie_legislativa'] = id_especie_legislativa
        entry['indicador_autor_principal'] = tgd(autoria, 'IndicadorAutorPrincipal')
        entry['numero_ordem_autor'] = tgd(autoria, 'NumeroOrdemAutor','int')
        entry['indicador_outros_autores'] = tgd(autoria, 'IndicadorOutrosAutores')

        return entry

    def update_DB(self,iterable, entry_columns,id_parlamentar):
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_autoria FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]

        if not isinstance(iterable,list):
            iterable = [iterable]

        for autoria in tqdm(iterable):
            entry = self.fill_entry_parlamentar_autoria(autoria,entry_columns)
            id_especie_legislativa = entry['id_especie_legislativa']
            id_autoria = utils.generate_hash(id_parlamentar,id_especie_legislativa)

            entry['id_autoria'] = id_autoria

            if id_autoria not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_autoria)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_autoria', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_autoria',
                     'id_especie_legislativa',
                     'indicador_autor_principal',
                     'numero_ordem_autor',
                     'indicador_outros_autores',
                     'id_parlamentar',
                     'data_captura',
                     'url_captura']

    conn = utils.connect_sqlalchemy()
    list_of_parlamentares = conn.execute('SELECT id_parlamentar,id_senado FROM c_congresso.parlamentar_detalhe')

    for (id_parlamentar,id_senado) in tqdm(list_of_parlamentares):

        cap = Parlamentar_Autoria_Senado(table='parlamentar_autoria', schema='c_congresso', entry_columns=entry_columns)
        cap.entry_columns['id_parlamentar'] = id_parlamentar

        url = 'http://legis.senado.leg.br/dadosabertos/senador/{}/autorias'.format(id_senado)
        cap.entry_columns['url_captura'] = url
        data_source = utils.get_json(url)

        try:
            iterable = data_source['MateriasAutoriaParlamentar']['Parlamentar']['Autorias']['Autoria']
            cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns, id_parlamentar=id_parlamentar)
        except KeyError:
            continue