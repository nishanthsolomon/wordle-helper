from database.postgresql_utils import PostgresqlUtils
from database.config import config


if __name__ == "__main__":

    database_params = config(section='database')
    warm_start = True if database_params['warm_start'] == 'True' else False

    if not warm_start:
        PostgresqlUtils(False)
