from tqdm import tqdm
import pandas as pd
import copy
import datetime
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

class Sessoes_Legislativas_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_sessao_legislativa(self, sessao_legislativa, entry_columns):
        tgd = utils.try_get_data

        data_inicio = sessao_legislativa['DataInicio']
        sigla_casa = 'SF'
        id_sessao_legislativa = utils.generate_hash(data_inicio,sigla_casa)

        entry = copy.deepcopy(entry_columns)
        entry['id_sessao_legislativa'] = id_sessao_legislativa
        entry['sigla_casa'] = sigla_casa
        entry['data_inicio'] = datetime.datetime.strptime(data_inicio, "%Y-%m-%d").date()
        entry['data_fim'] = tgd(sessao_legislativa, 'DataFim', 'date')
        entry['data_inicio_intervalo'] = tgd(sessao_legislativa, 'DataInicioIntervalo', 'date')
        entry['data_fim_intervalo'] = tgd(sessao_legislativa, 'DataFimIntervalo', 'date')
        entry['tipo'] = tgd(sessao_legislativa,'TipoSessaoLegislativa')
        entry['data_captura'] = datetime.datetime.today()
        entry['api_captura'] = 'SF'

        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :return: None
        """
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_sessao_legislativa FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]

        if isinstance(iterable,dict):
            iterable = [iterable]

        for sessao_legislativa in tqdm(iterable):

            entry = self.fill_entry_sessao_legislativa(sessao_legislativa,entry_columns)
            id_sessao_legislativa = entry['id_sessao_legislativa']

            if id_sessao_legislativa not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_sessao_legislativa)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_sessao_legislativa', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_sessao_legislativa',
                     'sigla_casa',
                     'data_inicio',
                     'data_fim',
                     'data_inicio_intervalo',
                     'data_fim_intervalo',
                     'tipo',
                     'id_legislatura',
                     'data_captura',
                     'url_captura']

    cap = Sessoes_Legislativas_Senado(table='sessoes_legislativas', schema='c_congresso', entry_columns=entry_columns)
    url = 'http://legis.senado.leg.br/dadosabertos/plenario/lista/legislaturas'
    data_source = utils.get_json(url)
    legislaturas = data_source['ListaLegislatura']['Legislatura']['Legislatura']
    cap.entry_columns['url_captura'] = url

    for legislatura in tqdm(legislaturas):

        conn = utils.connect_sqlalchemy()
        id_api = legislatura['@id']
        id_legislatura = list(conn.execute('SELECT id_legislatura FROM c_congresso.legislaturas WHERE id_api = {}'.format(id_api)))[0][0]
        cap.entry_columns['id_legislatura'] = id_legislatura

        iterable = legislatura['SessoesLegislativas']['SessaoLegislativa']
        cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)

