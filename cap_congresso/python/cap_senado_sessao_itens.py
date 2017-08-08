from tqdm import tqdm
import pandas as pd
import copy
import datetime
import sys
import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')
capture = imp.load_source('capture','/congresso-em-numeros/capture.py')
Capture = capture.Capture

def main():
    if len(sys.argv) == 3:
        return sys.argv[1], sys.argv[2]
    else:
        print('You need inform the start date and end date of the capture.')


class Sessao_Itens_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_item(self, item, entry_columns):
        tgd = utils.try_get_data
        entry = copy.deepcopy(entry_columns)

        id_api_item = item['codigoItem']
        id_sessao = entry_columns['id_sessao']

        id_api_materia = item['codigoMateria']
        api_captura = 'SF'
        materia = self.conn.execute("SELECT id_especie_legislativa FROM c_congresso.especies_legislativas WHERE id_api={} and api_captura='{}'".format(id_api_materia,api_captura))
        try:
            id_especie_legislativa = list(materia)[0][0]
        except IndexError:
            id_especie_legislativa = ''

        id_item = utils.generate_hash(id_api_item,id_sessao,id_especie_legislativa)

        entry['id_item'] = id_item
        entry['id_api'] = int(id_api_item)
        entry['api_captura'] = api_captura
        entry['id_especie_legislativa'] = id_especie_legislativa
        entry['data_captura'] = datetime.datetime.today()
        entry['identificacao'] = tgd(item,'identificacao')
        entry['apreciacao_papeleta'] = tgd(item,'apreciacaoPapeleta')
        entry['ementa_papeleta'] = tgd(item,'ementaPapeleta')
        entry['cabecalho'] = tgd(item,'cabecalho')
        entry['indicador_deliberacao'] = tgd(item,'indicadorDeliberacao','int')
        entry['descricao_deliberacao'] = tgd(item,'descricaoDeliberacao')
        entry['sequencial_item'] = tgd(item,'sequencialItem','int')
        entry['tipo_pauta'] = tgd(item,'tipoPauta','int')
        entry['descricao_tipo_pauta'] = tgd(item,'descricaoTipoPauta')
        entry['indicador_revisado'] = tgd(item,'indicadorRevisado')
        entry['codigo_tipo_apreciacao'] = tgd(item,'codigoTipoApreciacao','int')
        entry['descricao_tipo_apreciacao'] = tgd(item,'descricaoTipoApreciacao')
        return entry

    def update_DB(self,iterable, entry_columns):
        """
        :param iterable: dict
        :param entry_columns: dict
        :param data: str -> 'sessoes' or 'itens'
        :return:
        """

        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_item FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for data_source in iterable:

            entry = self.fill_entry_item(data_source,entry_columns)
            id_item = entry['id_item']

            if id_item not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_item)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_item', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_item',
                     'id_especie_legislativa',
                     'id_sessao',
                     'id_item',
                     'identificacao',
                     'apreciacao_papeleta',
                     'ementa_papeleta',
                     'cabecalho',
                     'indicador_deliberacao',
                     'descricao_deliberacao',
                     'sequencial_item',
                     'tipo_pauta',
                     'descricao_tipo_pauta',
                     'indicador_revisado',
                     'codigo_tipo_apreciacao',
                     'descricao_tipo_apreciacao',
                     'data_captura',
                     'url_captura',
                     'api_captura']

    cap = Sessao_Itens_Senado(table='sessao_itens', schema='c_congresso', entry_columns=entry_columns)

    start_date, end_date = main()
    for date in tqdm(utils.daterange(start_date,end_date,convert=1,pattern='%d-%m-%Y')):
        d = date.strftime('%Y%m%d')
        url = 'http://legis.senado.leg.br/dadosabertos/plenario/resultado/{}'.format(d)
        data_source = utils.get_json(url)
        try:
            sessoes = data_source['ResultadoPlenario']['Sessoes']['Sessao']
            if isinstance(sessoes,dict):
                sessoes = [sessoes]

            for sessao in sessoes:
                iterable = sessao['Itens']['Item']
                if isinstance(iterable, dict):
                    iterable = [iterable]
                print(sessao)
                query = "SELECT id_sessao FROM c_congresso.sessoes WHERE id_api={} and api_captura='SF'".format(sessao['codigoSessao'])
                print(query)
                id_sessao = list(cap.conn.execute(query))[0][0]

                cap.entry_columns['url_captura'] = url
                cap.entry_columns['id_sessao'] = id_sessao
                cap_itens.update_DB(iterable=list_of_itens, entry_columns=cap_itens.entry_columns, data='itens')

        except KeyError:
            pass

