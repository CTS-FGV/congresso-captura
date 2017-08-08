import requests
import copy
import collections
import psycopg2
import yaml
import datetime
import time
import pandas as pd


def slack_notify_dag_error(context):
    """
    Slack notification when a DAG fail.
    This function should be used in on_failure_callback parameter of BashOperator.
    """

    dag_id = str(context['dag'])
    dag_id = dag_id[6:-1]

    task_id = str(context['task'])
    task_id = task_id[21:-1]

    dia = context['ds']

    hora = context['ts'][11:]

    link= "http://172.16.4.227:8080/admin/airflow/log?task_id={}&dag_id={}&execution_date={}".format(task_id,dag_id,context['ts'])

    mensagem = "DAG_ID: {} \n TASK_ID: {} \n DATA: {} \n HORA: {}".format(dag_id,task_id,dia,hora)

    data = {"attachments":[
                {"fallback": "Deu Erro! Fale com @alifersales",
                 "title": "Erro ao executar DAG",
                 "title_link": link,
                 "fields": [
                     {"value": mensagem}
                     ],
                 "color": "#ff0000"}]}

    data = str(data)

    headers = {'Content-type': 'application/json'}

    requests.post('https://hooks.slack.com/services/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', headers=headers, data=data)

def _strip_all(dic):
    """
    Strip values deleting spaces, \t and \n chars.
    :param dic:
    :return:
    """
    for k, v in dic.items():

        if len(v) == 0:
            dic[k] = 'NULL'
        if isinstance(v, str):
            v = v.strip().replace('\t', '').replace('\n', '').encode('utf-8', 'ignore')
            dic[k] = v

    return dic

def _get_values(full_dic, all_keys, skeleton):
    """
    Insert values from the xml to the skeleton dict using the paths stored in a skeleton tuple
    :param full_dic: dict:: Xml dict
    :param all_keys: List of Tuples :: Path to all variables
    :param skeleton: dict :: SQL skeleton
    :return:
    """
    for key_list in all_keys:

        # try to see if tree exists on xml
        tree_dic = copy.copy(full_dic)

        for l in key_list:
            try:
                # caso que value do dict é lista
                if isinstance(tree_dic, list):
                    tree_dic = tree_dic[0]
                tree_dic = tree_dic[l]
            except KeyError:
                pass


        if isinstance(tree_dic, list): # caso que value do dict é lista
            tree_dic = tree_dic[0]

        # if it exists, add values to dic
        for k, v in tree_dic.items():
            if isinstance(v, dict) or isinstance(v, list) or len(key_list) == 0:
                continue

            column = key_list[-1] + '_' + k
            if column in skeleton.keys():
                skeleton[column] = v

    return skeleton


"""
METHODS TO GENERATE THE SKELETON
"""
def get_value_on_dict(dic, tup):
    """
    Get a value in a dict using a tuple with the path
    :param dic: dictionary
    :param tup: tuple with ordered keys
    :return: value that the tuple is pointing to
    """
    d = dic
    for t in tup:
        if isinstance(d, list):
            d = d[0]
        d = d[t]
    return d



def iter_paths(tree, parent_path=()):
    """
    Get a dict map
    :param tree: dictionry
    :param parent_path:
    :return: Yelds the a list of tuples of all branches of a dict
    """
    for path, node in tree.items():
        current_path = parent_path + (path,)
        if isinstance(node, collections.Mapping):
             for inner_path in iter_paths(node, current_path):
                yield inner_path
        elif isinstance(node, list):
            for n in node:
                for inner_path in iter_paths(n, current_path):
                    yield inner_path
        else:
            yield current_path

def infer_type(value):
    """
    Infer the sql type of a python type. NOT IN USE
    :param value: a value
    :return: str :: SQL type
    """
    try:
        int(value)
        return 'TEXT'
    except:
        return 'TEXT'


def generate_skeleton(tree):
    """
    Generate skeleton to sql
    :param tree:
    :return:
    """
    skeleton = []
    for elem in tree:
        elem = elem[-3:]
        try:
            elem = (elem[0] + '_' + elem[1], elem[2])
        except IndexError:
            pass

        skeleton.append(elem)

    skeleton.append(('datacaptura', 'TIMESTAMP'))
    skeleton.append(('numerocaptura', 'INT  '))

    return skeleton

def connect_psycopg2():

    with open('./config_server.yaml', 'r') as f:
        server = yaml.load(f)

    host = server['host']
    database = server['database']
    user = server['user']
    password = server['password']

    conn = psycopg2.connect(
        host=host, port=5432, database=database,
        user=user, password=password)
    return conn

def connect_sqlalchemy():
    with open('/cron-jobs/captura/cn-database/cn_database/config_server.yaml', 'r') as f:
        server = yaml.load(f)

    host = server['host']
    database = server['database']
    user = server['user']
    password = server['password']

    from sqlalchemy import create_engine
    url = 'postgresql://{}:{}@{}/{}'
    url = url.format(user, password, host, database)
    return create_engine(url)

def logging(script, cursor, nextcapnum, detalhes, log_table, quantidade=False):


    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


    if quantidade:
        cursor.execute(
            "INSERT INTO {} (capnum, script, datahora, detalhes, quantidade) "
            "VALUES ({}, '{}', '{}', '{}', '{}')".format(log_table, nextcapnum, script, st, detalhes, int(quantidade)))
    else:
        cursor.execute(
            "INSERT INTO {} (capnum, script, datahora, detalhes) "
            "VALUES ({}, '{}', '{}', '{}')".format(log_table, nextcapnum, script, st, detalhes))

if __name__ == '__main__':
    pass
