# -*- coding: utf-8 -*-
import requests
import luigi
import xmltodict
import luigi.contrib.postgres
import yaml
import pickle
import datetime
import collections
import random as rd

import cn_database.utils as utils

class Download(luigi.Task):
    """
    Downloads xml file from senado API

    id: materia_id given by Senado
    """

    id = luigi.IntParameter()

    def output(self):
        with open('./cn_database/config.yaml', 'r') as f:
            config = yaml.load(f)
        return luigi.LocalTarget(config['data_path'] + 'materia/materia_{}.xml'.format(self.id))

    def run(self):
        result = requests.get('http://legis.senado.leg.br/dadosabertos/materia/{}'.format(self.id))

        if result.status_code == 200:
            with self.output().open('w') as f:
                r = result.text
                f.write(r)

class Skeleton(luigi.Task):
    """
    Builds xml skeleton by downloading several samples.

    It is needed because Senado's XML structure can vary.

    It gets 1000 elements from the full set to build the skeleton
    """

    def requires(self):
        return [Download(i) for i in rd.sample(range(130000), 1000)]

    def output(self):
        return [luigi.LocalTarget('cn_database/skeletons/senado_materia_keymap.p'),
                luigi.LocalTarget('cn_database/skeletons/senado_materia_sql.p')]

    def run(self):

        tree = []
        key_map = []
        for file in self.input():
            with file.open('rb') as f:
                xml = f.read()

                try:
                    full_dic = xmltodict.parse(xml)['DetalheMateria']['Materia']
                except KeyError:
                    raise Exception('EmptyXML')

                keys = list(utils.iter_paths(full_dic))

                for elem in keys:
                    sql_type = utils.infer_type(utils.get_value_on_dict(full_dic, elem))
                    elem_type = (elem + (sql_type,))
                    if elem_type not in tree:
                        tree.append(elem_type)

                    elem_map = elem[:-1]
                    if elem_map not in key_map:
                        key_map.append(elem_map)


        skeleton = utils.generate_skeleton(tree)

        skeleton = sorted(list(set(skeleton)))

        pickle.dump(key_map, open(self.output()[0].path, 'wb'))

        pickle.dump(skeleton, open(self.output()[1].path, 'wb'))

class Parse(luigi.Task):
    """
    Gets a XML file and parses it to a .tsv file using the skeleton.

    Raises an error if XML is empty
    """

    id = luigi.IntParameter()

    def requires(self):
        return [Skeleton(), Download(self.id)]

    def output(self):
        with open('./cn_database/config.yaml', 'r') as f:
            config = yaml.load(f)
        return luigi.LocalTarget(config['data_path'] + 'materia/materia_{}.tsv'.format(self.id))

    def run(self):
        columns = pickle.load(open('cn_database/skeletons/senado_materia_sql.p', 'rb'))

        d = {c[0]:""  for c in columns}

        skeleton = collections.OrderedDict(sorted(d.items(), key=lambda x: x[0]))

        with self.input()[1].open('rb') as f:
            xml = f.read()
            df = self._to_csv(xml, skeleton)

        with self.output().open('w') as f:

            for i, v in enumerate(df.values()):
                if len(df)-1 == i:
                    print('funcionou')
                    f.write('{}'.format(v))
                else:
                    f.write('{}\t'.format(v))

            #df.to_csv(f, index=None, enconding='utf8', sep='\t', header=False)


    def _to_csv(self, xml, skeleton):

        # transforma xml em OrderedDict
        try:
            full_dic = xmltodict.parse(xml)['DetalheMateria']['Materia']
            all_keys = pickle.load(open('cn_database/skeletons/senado_materia_keymap.p', 'rb')) # Enuncia o ramo que as variáveis estão
            df = utils._get_values(full_dic, all_keys, skeleton) # Completa o skeleton com informações dos dics

        except KeyError:
            raise Exception('EmptyXML')

        df['datacaptura'] = datetime.datetime.now()

        df['numerocaptura'] = int(1)

        df = utils._strip_all(df)

        return df

class RawSql(luigi.contrib.postgres.CopyToTable):
    """
    Insert the .tsv to the database. Uses skeleton as sql columns
    """
    id = luigi.IntParameter()

    with open('./cn_database/config_server.yaml', 'r') as f:
        server = yaml.load(f)


    host = server['host']
    database = server['database']
    user = server['user']
    password = server['password']
    table = "c_senado.senado_materia"
    try:
        columns = pickle.load(open('cn_database/skeletons/senado_materia_sql.p', 'rb'))
    except:
        pass

    # allows same xml to be uploaded twice
    @property
    def update_id(self):
        return str(datetime.datetime.now())

    def requires(self):
        return Parse(self.id)

class GetAll(luigi.Task):
    """
    Method that calls the pipeline for a range of materias
    """
    init = luigi.IntParameter()
    interval  = luigi.IntParameter()
    def requires(self):
        return [RawSql(id) for id in range(self.init, self.init + self.interval, 1)]

