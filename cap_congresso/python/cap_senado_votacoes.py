from tqdm import tqdm
import pandas as pd
import copy
import sys
import datetime
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

def main():
    if len(sys.argv) == 3:
        return sys.argv[1], sys.argv[2]
    else:
        print('You need inform the start date and end date of the capture.')


class Votacoes_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_votacao(self, votacao, entry_columns):
        tgd = utils.try_get_data

        codigo_sessao_votacao = votacao['CodigoSessaoVotacao']

        api_captura = "SF"
        id_votacao = utils.generate_hash(codigo_sessao_votacao,api_captura)

        entry = copy.deepcopy(entry_columns)
        entry['id_votacao'] = id_votacao
        entry['api_captura'] = api_captura
        entry['codigo_sessao_votacao'] = int(codigo_sessao_votacao)
        entry['indicador_votacao_secreta'] = tgd(votacao,'Secreta')
        entry['descricao_votacao'] = tgd(votacao, 'DescricaoVotacao')
        entry['resultado'] = tgd(votacao, 'Resultado')
        entry['data_captura'] = datetime.datetime.today()

        return entry

    def insert_relation_votacao_item(self,votacao,id_votacao):
        print(votacao)
        api_captura = 'SF'
        id_especie_legislativa_api = votacao['CodigoMateria']
        query = "SELECT id_especie_legislativa FROM c_congresso.especies_legislativas WHERE api_captura='{}' AND id_api={}".format(api_captura,id_especie_legislativa_api)
        print(query)
        id_especie_legislativa = self.conn.execute(query)
        id_especie_legislativa = list(id_especie_legislativa)[0][0]

        codigo_sessao = votacao['CodigoSessao']
        id_sessao = utils.generate_hash(codigo_sessao,'SF')
        query = "SELECT id_item FROM c_congresso.sessao_itens WHERE id_sessao='{}' AND id_especie_legislativa='{}'".format(id_sessao, id_especie_legislativa)
        print(query)
        id_item = self.conn.execute(query)
        id_item = list(id_item)[0][0]

        conn.execute("INSERT INTO c_congresso.votacoes_itens VALUES ('{}','{}')".format(id_votacao,id_item))

        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :return:
        """
        conn = self.conn
        bulk_votacao = []
        list_of_id_db = conn.execute('SELECT id_votacao FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for votacao in tqdm(iterable):

            entry = self.fill_entry_votacao(votacao,entry_columns)
            id_votacao = entry['id_votacao']

            if id_votacao not in list_of_id_db:
                bulk_votacao.append(entry)
                self.insert_relation_votacao_item(votacao,id_votacao)
                list_of_id_db.append(id_votacao)

        if len(bulk_votacao) > 0:
            df = pd.DataFrame(bulk_votacao)
            df.set_index('id_votacao', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_votacao',
                     'api_captura',
                     'codigo_sessao_votacao',
                     'indicador_votacao_secreta',
                     'descricao_votacao',
                     'resultado',
                     'data_captura',
                     'url_captura', ]


    start_date, end_date = main()
    for date in tqdm(utils.daterange(start_date,end_date,True,'%d-%m-%Y')):
        cap = Votacoes_Senado(table='votacoes', schema='c_congresso', entry_columns=entry_columns)
        d = date.strftime('%Y%m%d')
        url = 'http://legis.senado.leg.br/dadosabertos/plenario/lista/votacao/{}'.format(d)
        data_source = utils.get_json(url)
        try:
            iterable = data_source['ListaVotacoes']['Votacoes']['Votacao']
            if isinstance(iterable,dict):
                iterable = [iterable]
            cap.entry_columns['url_captura'] = url
            cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)
        except KeyError:
            pass
