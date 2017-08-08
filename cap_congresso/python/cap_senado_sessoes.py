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


class Sessoes_senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_sessao(self, sessao, entry_columns):
        tgd = utils.try_get_data

        codigo_sessao = sessao['CodigoSessao']
        api_captura = 'SF'

        id_sessao = utils.generate_hash(codigo_sessao,api_captura)

        entry = copy.deepcopy(entry_columns)
        entry['id_sessao'] = id_sessao
        entry['data_captura'] = datetime.datetime.today()
        entry['api_captura'] = api_captura
        entry['id_api'] = int(codigo_sessao)
        entry['sigla_casa'] = tgd(sessao,'SiglaCasa')
        entry['numero_sessao'] = tgd(sessao,'NumeroSessao','int')
        entry['tipo_sessao'] = tgd(sessao,'TipoSessao')
        entry['descricao_tipo_sessao'] = tgd(sessao,'DescricaoTipoSessao')

        data_sessao = tgd(sessao, 'DataSessao', 'date')
        hora_sessao = tgd(sessao, 'HoraInicio', 'time')

        if data_sessao and hora_sessao:
            entry['datetime_sessao'] = datetime.datetime.combine(data_sessao, hora_sessao)
            query = list(self.conn.execute("SELECT id_sessao_legislativa FROM c_congresso.sessoes_legislativas WHERE data_inicio <= '{}' AND data_fim >= '{}'".format(data_sessao,data_sessao)))
            entry['id_sessao_legislativa'] = query[0][0]

        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :return:
        """
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_sessao FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for sessao in tqdm(iterable):

            entry = self.fill_entry_sessao(sessao,entry_columns)
            id_sessao = entry['id_sessao']

            if id_sessao not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_sessao)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_sessao', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_sessao',
                     'id_api',
                     'datetime_sessao',
                     'numero_sessao',
                     'tipo_sessao',
                     'descricao_tipo_sessao',
                     'sigla_casa',
                     'id_sessao_legislativa',
                     'data_captura',
                     'url_captura',
                     'api_captura']

    start_date, end_date = main()
    for date in tqdm(utils.daterange(start_date,end_date,True,'%d-%m-%Y')):
        cap = Sessoes_senado(table='sessoes', schema='c_congresso', entry_columns=entry_columns)
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