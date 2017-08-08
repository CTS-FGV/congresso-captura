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
    if len(sys.argv) == 2:
        return sys.argv[1]
    else:
        print('You need inform if this function will update (True) or create (False) the table.')

class Senadores(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_senador(self,senador,entry_columns):
        tgd = utils.try_get_data

        id_senador = senador['IdentificacaoParlamentar']['CodigoParlamentar']
        api_captura = "SF"

        id_parlamentar = utils.generate_hash(id_senador, api_captura)

        senador_dados_basicos = utils.get_json('http://legis.senado.leg.br/dadosabertos/senador/{}'.format(id_senador))
        senador_dados_basicos = tgd(senador_dados_basicos['DetalheParlamentar']['Parlamentar'],
                                    'DadosBasicosParlamentar')

        entry = copy.deepcopy(entry_columns)
        entry['id_parlamentar'] = id_parlamentar

        if senador_dados_basicos:

            entry['data_nascimento'] = tgd(senador_dados_basicos, 'DataNascimento','date')
            entry['data_falecimento'] = tgd(senador_dados_basicos, 'DataFalecimento','date')
            entry['fone'] = tgd(senador_dados_basicos, 'TelefoneParlamentar')
            entry['fax'] = tgd(senador_dados_basicos, 'FaxParlamentar')
            entry['endereco_congresso'] = tgd(senador_dados_basicos, 'EnderecoParlamentar')

        entry['data_captura'] = datetime.datetime.today()
        entry['api_captura'] = api_captura
        entry['id_senado'] = int(id_senador)
        entry['nome_completo'] = senador['IdentificacaoParlamentar']['NomeCompletoParlamentar']
        entry['nome_parlamentar_atual'] = tgd(senador['IdentificacaoParlamentar'], 'NomeParlamentar')
        entry['forma_tratamento'] = senador['IdentificacaoParlamentar']['FormaTratamento']
        entry['sexo_parlamentar'] = senador['IdentificacaoParlamentar']['SexoParlamentar']
        entry['email'] = tgd(senador['IdentificacaoParlamentar'], 'EmailParlamentar')
        entry['sigla_uf_origem'] = tgd(senador['IdentificacaoParlamentar'], 'UfParlamentar')
        entry['website'] = tgd(senador['IdentificacaoParlamentar'], 'UrlPaginaParlamentar')

        if isinstance(senador['Mandatos']['Mandato'],list):

            ultimo_mandato = senador['Mandatos']['Mandato'][0]
            for mandato in senador['Mandatos']['Mandato'][1:]:
                fim_mandato = mandato['SegundaLegislaturaDoMandato']['DataFim']
                fim_mandato = datetime.datetime.strptime(fim_mandato, "%Y-%m-%d").date()
                fim_ultimo_mandato = ultimo_mandato['SegundaLegislaturaDoMandato']['DataFim']
                fim_ultimo_mandato = datetime.datetime.strptime(fim_ultimo_mandato, "%Y-%m-%d").date()

                if fim_mandato > fim_ultimo_mandato:
                    ultimo_mandato = mandato
            entry['descricao_participacao'] = tgd(ultimo_mandato,'DescricaoParticipacao')
        else:
            entry['descricao_participacao'] = tgd(senador['Mandatos']['Mandato'], 'DescricaoParticipacao')

        return entry

    def lista_atual(self):
        list_of_ids = []
        data_source = utils.get_json('http://legis.senado.leg.br/dadosabertos/senador/lista/atual')

        for senador in data_source['ListaParlamentarEmExercicio']['Parlamentares']['Parlamentar']:
            id_senador = senador['IdentificacaoParlamentar']['CodigoParlamentar']
            nome_completo = senador['IdentificacaoParlamentar']['NomeCompletoParlamentar']

            id_parlamentar = utils.generate_hash(id_senador, nome_completo)
            list_of_ids.append(id_parlamentar)

        return list_of_ids

    def lista_afastados(self):
        list_of_ids = []
        data_source = utils.get_json('http://legis.senado.leg.br/dadosabertos/senador/lista/afastados')

        for senador in data_source['AfastamentoAtual']['Parlamentares']['Parlamentar']:
            id_senador = senador['IdentificacaoParlamentar']['CodigoParlamentar']
            nome_completo = senador['IdentificacaoParlamentar']['NomeCompletoParlamentar']

            id_parlamentar = utils.generate_hash(id_senador, nome_completo)
            list_of_ids.append(id_parlamentar)

        return list_of_ids

    def update_DB(self, iterable, entry_columns, update):
        """
         Capture and insert the list of 'senadores atuais'.
         :param iterable: list | list of data sources.
         :param entry_structure: dict | Structure of the Database.
         :param update: bool | True if table already exist.
         :return: None
         """
        conn = self.conn
        bulk = []
        old_bulk = []
        list_of_id_db = list()
        list_of_id_atuais = self.lista_atual()
        list_of_id_afastados = self.lista_afastados()

        if update:
            list_of_id_db = conn.execute('SELECT id_parlamentar FROM {}.{}'.format(self.schema, self.table))
            list_of_id_db = [tup[0] for tup in list_of_id_db]
            id_row_historic = list(conn.execute('SELECT MAX(id) FROM {}.{}_historic'.format(self.schema, self.table)))[0][0]
            if not id_row_historic:
                id_row_historic = 0

        for senador in tqdm(iterable):
            entry = self.fill_entry_senador(senador,entry_columns)
            id_parlamentar = entry['id_parlamentar']

            if id_parlamentar in list_of_id_atuais:
                entry['situacao_parlamentar'] = 'atual'
            elif id_parlamentar in list_of_id_afastados:
                entry['situacao_parlamentar'] = 'afastado'

            if id_parlamentar in list_of_id_db:
                compare_columns = 'id_parlamentar, nome_completo, nome_parlamentar_atual, forma_tratamento, sexo_parlamentar, data_nascimento, data_falecimento, sigla_uf_origem, endereco_origem, nome_cidade_origem, codigo_estado_civil, endereco_congresso, fone, fax, website, email, profissao, id_camara, id_senado, cpf, titulo_de_eleitor, descricao_participacao'

                old_row = conn.execute("SELECT {} FROM {}.{} WHERE id_parlamentar='{}'".format(compare_columns,self.schema, self.table,id_parlamentar))
                old_row = list(old_row)[0]
                new_row = tuple([entry[column] for column in compare_columns.split(', ')])

                if old_row != new_row:
                    old_entry = copy.deepcopy(entry_columns)

                    for key in old_entry.keys():
                        old_date = conn.execute("SELECT {} FROM {}.{} WHERE id_parlamentar='{}'".format(key,self.schema, self.table,id_parlamentar))
                        old_entry[key] = list(old_date)[0][0]
                    old_entry['change_date']  = datetime.datetime.today() #capture of change date
                    id_row_historic += 1
                    old_entry['id'] = id_row_historic

                    old_bulk.append(old_entry)
                    conn.execute("DELETE FROM {}.{} WHERE id_parlamentar='{}'".format(self.schema, self.table,id_parlamentar))

                    bulk.append(entry)
            else:
                bulk.append(entry)

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_parlamentar', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

        if len(old_bulk) > 0:
            df2 = pd.DataFrame(old_bulk)
            df2.set_index('id_parlamentar', inplace=True)
            historic_table_name = self.table + '_historic'
            print('Adding {} entries to SQL table {}.{}.'.format(len(df2),self.schema, historic_table_name))
            df2.to_sql(historic_table_name, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == "__main__":

    entry_columns = [ 'id_parlamentar',
                      'nome_completo',
                      'nome_parlamentar_atual',
                      'forma_tratamento',
                      'sexo_parlamentar',
                      'data_nascimento',
                      'data_falecimento',
                      'sigla_uf_origem',
                      'endereco_origem',
                      'nome_cidade_origem',
                      'codigo_estado_civil',
                      'endereco_congresso',
                      'fone',
                      'fax',
                      'website',
                      'email',
                      'profissao',
                      'data_captura',
                      'url_captura',
                      'id_camara',
                      'id_senado',
                      'cpf',
                      'titulo_de_eleitor',
                      'descricao_participacao',
                      'situacao_parlamentar']

    update = main()
    if update in ['True','1']:
        update = True
    elif update in ['False','0']:
        update = False

    if isinstance(update,bool):
        cap = Senadores(table='parlamentar_detalhe',schema='c_congresso',entry_columns=entry_columns)
        url = 'http://legis.senado.leg.br/dadosabertos/senador/lista/legislatura/1/60'
        data_source = utils.get_json(url)
        iterable = data_source['ListaParlamentarLegislatura']['Parlamentares']['Parlamentar']
        cap.entry_columns['url_captura'] = url
        cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns, update=update)
    else:
        print('The param must be bool.')