import psycopg2
from database.config import config
from database.postgresql_tables import Tables
import logging

logger = logging.getLogger(__name__)


class PostgresqlUtils():

    INSERT_COMMAND = 'INSERT INTO {} ({}) VALUES({}) RETURNING {}'
    SELECT_COMMAND_CONDITION = 'SELECT {} FROM {} {}'
    WHERE_CLAUSE = 'WHERE {} = {}'
    BETWEEN_CLAUSE = 'WHERE {} BETWEEN \'{}\' AND \'{}\''
    UPDATE_COMMAND = 'UPDATE {} SET {} WHERE {} = {}'
    DELETE_COMMAND = 'DELETE FROM {} WHERE {} = {}'
    ASTERISK_ALL_COLUMNS = '*'

    def __init__(self, warm_start=True):
        self.params = config(section='postgresql')
        self.tables = Tables()

        if not warm_start:
            self.create_tables()
            logger.info('Created Tables!')
        
        logger.info('Postgresql initialized')

    def create_tables(self):
        try:
            connection = psycopg2.connect(**self.params)
            cursor = connection.cursor()

            for create_table_command in self.tables.get_create_table_commands():
                cursor.execute(create_table_command)

            cursor.close()
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()

    def insert_values(self, table_name, values):

        insert_column_names = ', '.join(
            self.tables.get_insert_column_names(table_name))
        primary_key = self.tables.get_primary_key(table_name)

        insert_command = PostgresqlUtils.INSERT_COMMAND.format(
            table_name, insert_column_names, values, primary_key)

        try:
            connection = psycopg2.connect(**self.params)
            cursor = connection.cursor()

            cursor.execute(insert_command)
            new_id = cursor.fetchone()[0]

            cursor.close()
            connection.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()
        row = self.get_row(table_name, primary_key, new_id)
        return row

    def get_all_rows(self, table_name):

        return self.get_rows(table_name, PostgresqlUtils.ASTERISK_ALL_COLUMNS, '')

    def get_all_rows_specific_column(self, table_name, column_name):

        return self.get_rows(table_name, column_name, '')

    def get_rows_range(self, table_name, range_column_name, lower_bound_value, upper_bound_value):
        between_clause = PostgresqlUtils.BETWEEN_CLAUSE.format(
            range_column_name, lower_bound_value, upper_bound_value)

        return self.get_rows(table_name, PostgresqlUtils.ASTERISK_ALL_COLUMNS, between_clause)

    def get_column(self, table_name, select_column_name, range_column_name, lower_bound_value, upper_bound_value):
        between_clause = PostgresqlUtils.BETWEEN_CLAUSE.format(
            range_column_name, lower_bound_value, upper_bound_value)

        return self.get_rows(table_name, select_column_name, between_clause)

    def get_row(self, table_name, condition_column_name, condition_column_value):
        where_clause = PostgresqlUtils.WHERE_CLAUSE.format(
            condition_column_name, condition_column_value)

        return self.get_rows(table_name, PostgresqlUtils.ASTERISK_ALL_COLUMNS, where_clause)

    def get_row_id(self, table_name, primary_key_value):
        primary_key = self.tables.get_primary_key(table_name)
        where_clause = PostgresqlUtils.WHERE_CLAUSE.format(
            primary_key, primary_key_value)

        return self.get_rows(table_name, PostgresqlUtils.ASTERISK_ALL_COLUMNS, where_clause)

    def get_rows(self, table_name, select_column_name, conditional_clause):
        select_command = PostgresqlUtils.SELECT_COMMAND_CONDITION.format(
            select_column_name, table_name, conditional_clause)
        if select_column_name is PostgresqlUtils.ASTERISK_ALL_COLUMNS:
            columns = self.tables.get_column_names(table_name)
        else:
            columns = [select_column_name]

        try:
            connection = psycopg2.connect(**self.params)
            cursor = connection.cursor()

            cursor.execute(select_command)
            rows = cursor.fetchall()

            result_rows = []
            for row in rows:
                dict_row = {}
                for column, item in zip(columns, row):
                    dict_row[column] = item
                result_rows.append(dict_row)

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()

        return result_rows

    def update_row(self, table_name, primary_key_value, updated_columns):
        primary_key = self.tables.get_primary_key(table_name)
        update_command = PostgresqlUtils.UPDATE_COMMAND.format(
            table_name, updated_columns, primary_key, primary_key_value)

        try:
            connection = psycopg2.connect(**self.params)
            cursor = connection.cursor()

            cursor.execute(update_command)

            if (cursor.rowcount == 0):
                raise Exception('Row not found! Update not done.')

            cursor.close()
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()

        row = self.get_row(table_name, primary_key, primary_key_value)
        return row

    def delete_row(self, table_name, primary_key_value):
        primary_key = self.tables.get_primary_key(table_name)
        delete_command = PostgresqlUtils.DELETE_COMMAND.format(
            table_name, primary_key, primary_key_value)

        try:
            connection = psycopg2.connect(**self.params)
            cursor = connection.cursor()

            cursor.execute(delete_command)

            if (cursor.rowcount == 0):
                raise Exception('Row not found! Deletion not done.')

            cursor.close()
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if connection is not None:
                connection.close()

        return True
