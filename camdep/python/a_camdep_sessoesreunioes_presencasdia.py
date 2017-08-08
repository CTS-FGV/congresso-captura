import numpy as np
import pandas as pd


import imp

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')

CASA      = 'camdep'
API_TYPE  = 'sessoesreunioes'
API_NAME  = 'presencasdia2'


SCHEMA    = 'a_camdep'
TABLE  = ('_').join([CASA, API_TYPE, API_NAME])
SCRIPT = ('_').join([CASA, API_NAME, API_TYPE]) + '.py'

CONN = utils.connect_sqlalchemy()

def frequencia(presencas):

    frequencia2numero = {'Ausência': 0, 'Presença': 1, '--------': np.nan}
    presencas['frequencia_numero'] = presencas['frequencia'].apply(lambda x: frequencia2numero[x])

    return presencas


def tipoSessao2numero(x):
    """
    'EXTRAORDINÁRIA': 0,
    'ORDINÁRIA':1,
    'SESSÃO':2
    """
    if x[:4] == 'EXTR':
        return 0

    if x[:5] == 'ORDIN':
        return 1

    if x[:3] == 'SES':
        return 2

    else:
        raise ValueError('{} não é um tipo previsto de sessão'.format(x))

def tipoSessao(presencas):

    presencas['tipo_sessao_numero'] = presencas['tipo_sessao'].apply(tipoSessao2numero)

    return presencas

relacoes = {'SD'  : ['SDD', 'SOLIDARIED', 'SD'],
            'GOV' : ['APOIO AO GOVERNO', 'GOV', 'GOV.'],
            'DEM' : ['DEM', 'PFL'],
            'PR'  : ['PRONA', 'PL', 'PR', 'PST'],
            'PTB' : ['PAN', 'PTB'],
            'PRB' : ['PRB', 'PMR'],
            'PP'  : ['PP', 'PPB'],
            'PODE': ['PTN', 'PODE']}


def substitui_sigla(sigla):
    for final, values in relacoes.items():
        if sigla.upper() in values:
            return final
    else:
        return sigla.upper()

def partidos(presencas):

    presencas['sigla_partido_atualizado'] = presencas['sigla_partido'].apply(substitui_sigla)

    return presencas

def filter():
    """
    #### TRATAMENTO DOS DADOS
        - Tratar frequencia para numerico
        - Tratar tipoSessao para numerico
        - Tratar partidos
    :return:
    """
    presencas = pd.read_sql_query((
                                      "SELECT *\n"
                                      "FROM c_camdep.{}\n"
                                      "WHERE datacaptura = \n"
                                      "  (SELECT max(datacaptura) maxDate\n"
                                      "  FROM c_camdep.{} a)\n"
                                   ).format(('_').join([CASA, API_TYPE, API_NAME]),
                                            ('_').join([CASA, API_TYPE, API_NAME])),
                                  CONN)

    presencas = frequencia(presencas)
    presencas = tipoSessao(presencas)
    presencas = partidos(presencas)

    presencas.to_sql('camdep_sessoesreunioes_presencasdia', CONN, schema='a_camdep', if_exists='append')



if __name__ == '__main__':
    filter()