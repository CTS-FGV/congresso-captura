import datetime
import imp

utils = imp.load_source('utils','/Users/Admin/Documents/Projects/Congresso em Numeros/227/congresso-em-numeros/utils.py')


class Capture(object):

    def __init__(self,house=None, api_type=None, api_name=None, schema=None, entry_columns=None):
        """ Parent class for data capture.

        :param house: str
        :param api_type: str
        :param api_name: str
        :param schema: str
        :param entry_columns: list
        """
        self.house = house
        self.api_type = api_type
        self.api_name = api_name
        self.schema = schema


        self.table = ('_').join([self.house, self.api_type, self.api_name])
        self.conn = utils.connect_sqlalchemy()

        self.entry_structure = dict.fromkeys(entry_columns)

    def daterange(self, start_date, end_date):
        """
        Yelds days from start date to end date
        :param start_date: datetime
        :param end_date:  datetime
        :return: datetime
        """
        for n in range(int((start_date - end_date).days)):
            yield start_date - datetime.timedelta(n)
