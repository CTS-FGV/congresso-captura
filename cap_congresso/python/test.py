import imp
import datetime

utils = imp.load_source('utils','/congresso-em-numeros/utils.py')

if __name__ == "__main__":
    conn = utils.connect_sqlalchemy()
    d = datetime.datetime.strptime('2013-03-13','%Y-%m-%d').date()
    query = "SELECT data_inicio,data_fim FROM c_congresso.sessoes_legislativas WHERE data_inicio < '{}' AND data_fim > '{}'".format(d,d)
    print(list(conn.execute(query)))
