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
        print('You need inform the start year and end year of the capture.')

class Especies_Legislativas_Senado(Capture):

    def __init__(self,house=None,api_type=None,api_name=None,table=None,schema=None,entry_columns=None):
        Capture.__init__(self,house,api_type,api_name,table,schema,entry_columns)

    def fill_entry_especie_legislativa(self, materia, entry_columns):
        tgd = utils.try_get_data

        IdentificacaoMateria = materia['IdentificacaoMateria']
        DadosBasicosMateria = materia['DadosBasicosMateria']

        id_api = IdentificacaoMateria['CodigoMateria']
        api_captura = "SF"

        id_especie_legislativa = utils.generate_hash(id_api, api_captura)

        dados_materia = utils.get_json('http://legis.senado.leg.br/dadosabertos/materia/{}'.format(id_api))
        OrigemMateria = tgd(dados_materia['DetalheMateria'], 'OrigemMateria')
        CasaIniciadora = tgd(dados_materia['DetalheMateria'], 'CasaIniciadoraNoLegislativo')

        entry = copy.deepcopy(entry_columns)
        entry['id_especie_legislativa'] = id_especie_legislativa
        entry['id_api'] = int(id_api)
        entry['api_captura'] = api_captura
        entry['data_captura'] = datetime.datetime.today()

        entry['sigla_casa_identificacao'] = tgd(IdentificacaoMateria, 'SiglaCasaIdentificacaoMateria')
        entry['nome_casa_identificacao'] = tgd(IdentificacaoMateria, 'NomeCasaIdentificacaoMateria')
        entry['numero'] = tgd(IdentificacaoMateria, 'NumeroMateria', 'int')
        entry['ano'] = tgd(IdentificacaoMateria,'AnoMateria','int')
        entry['descricao_subtipo'] = tgd(IdentificacaoMateria, 'DescricaoSubtipoMateria')
        entry['sigla_subtipo'] = tgd(IdentificacaoMateria, 'SiglaSubtipoMateria')
        entry['indicador_tramitando'] = tgd(IdentificacaoMateria, 'IndicadorTramitando')

        entry['ementa'] = tgd(DadosBasicosMateria, 'EmentaMateria')
        entry['explicacao_ementa'] = tgd(DadosBasicosMateria, 'ExplicacaoEmentaMateria')
        entry['obsercacao'] = tgd(DadosBasicosMateria, 'ObservacaoMateria')
        entry['apelido'] = tgd(DadosBasicosMateria, 'ApelidoMateria')
        entry['indexacao'] = tgd(DadosBasicosMateria, 'IndexacaoMateria')
        entry['indicador_complementar'] = tgd(DadosBasicosMateria, 'IndicadorComplementar')
        entry['sigla_casa_leitura'] = tgd(DadosBasicosMateria, 'SiglaCasaLeitura')
        entry['nome_casa_leitura'] = tgd(DadosBasicosMateria, 'NomeCasaLeitura')
        entry['data_apresentacao'] = tgd(DadosBasicosMateria, 'DataApresentacao','date')
        entry['data_leitura'] = tgd(DadosBasicosMateria, 'DataLeitura','date')

        if OrigemMateria:
            entry['nome_poder_origem'] = tgd(OrigemMateria, 'NomePoderOrigem')
            entry['sigla_casa_origem'] = tgd(OrigemMateria, 'SiglaCasaOrigem')
            entry['nome_casa_origem'] = tgd(OrigemMateria, 'NomeCasaOrigem')

        if CasaIniciadora:
            entry['sigla_casa_iniciadora'] = tgd(CasaIniciadora, 'SiglaCasaIniciadora')
            entry['nome_casa_iniciadora'] = tgd(CasaIniciadora, 'NomeCasaIniciadora')

        return entry

    def update_DB(self,iterable, entry_columns):
        """

        :param iterable:
        :param entry_columns:
        :return:
        """
        conn = self.conn
        bulk = []
        list_of_id_db = conn.execute('SELECT id_especie_legislativa FROM {}.{}'.format(self.schema, self.table))
        list_of_id_db = [tup[0] for tup in list_of_id_db]
        for especie_legislativa in tqdm(iterable):
            entry = self.fill_entry_especie_legislativa(especie_legislativa,entry_columns)
            id_especie_legislativa = entry['id_especie_legislativa']

            if id_especie_legislativa not in list_of_id_db:
                bulk.append(entry)
                list_of_id_db.append(id_especie_legislativa)
            else:
                print(entry['id_api'])

        if len(bulk) > 0:
            df = pd.DataFrame(bulk)
            df.set_index('id_especie_legislativa', inplace=True)
            print('Adding {} entries to SQL table {}.{}.'.format(len(df),self.schema, self.table))
            df.to_sql(self.table, con=self.conn, schema=self.schema, if_exists='append')

if __name__ == '__main__':

    entry_columns = ['id_especie_legislativa',
                     'sigla_casa_identificacao',
                     'id_api',
                     'nome_casa_identificacao',
                     'sigla_subtipo',
                     'descricao_subtipo',
                     'numero',
                     'ano',
                     'indicador_tramitando',
                     'ementa',
                     'explicacao_ementa',
                     'obsercacao',
                     'apelido',
                     'indexacao',
                     'indicador_complementar',
                     'data_apresentacao',
                     'data_leitura',
                     'sigla_casa_leitura',
                     'nome_casa_leitura',
                     'nome_poder_origem',
                     'sigla_casa_origem',
                     'nome_casa_origem',
                     'sigla_casa_iniciadora',
                     'nome_casa_iniciadora',
                     'data_captura']

    start_year, end_year = main()
    try:
        start_year = int(start_year)
        end_year = int(end_year)
        verificador = True
    except ValueError:
        verificador = False
        print("Please, inform integers parameters.\nstructure: python3 cap_senado_especies_legislativa [start_year: int] [end_year: int]")

    if verificador:
        cap = Especies_Legislativas_Senado(table='especies_legislativas', schema='c_congresso', entry_columns=entry_columns)
        for year in tqdm(range(start_year,end_year+1)):
            url = 'http://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista?ano={}'.format(year)
            data_source = utils.get_json(url)
            try:
                iterable = data_source['PesquisaBasicaMateria']['Materias']['Materia']
                cap.entry_columns['url_captura'] = url
                if isinstance(iterable,dict):
                    iterable = [iterable]

                cap.update_DB(iterable=iterable, entry_columns=cap.entry_columns)
            except KeyError:
                continue