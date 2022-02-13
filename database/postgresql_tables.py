import xml.etree.ElementTree as ET


class Tables():

    NAME = 'name'
    DATA_TYPE = 'data_type'
    CONSTRAINTS = 'constraints'
    NOT_NULL = 'NOT NULL'
    CREATE_TABLE_COMMAND = 'CREATE TABLE {} ({})'
    PRIMARY_KEY = 'PRIMARY KEY'

    def __init__(self, file_path='../database/conf/data-dictionary.xml'):

        tree = ET.parse(file_path)
        self.tables = tree.getroot()
        self.table_columns_constraints_dict = {}

        self.__set_values()

    def __set_values(self):

        for table in self.tables:
            table_name = table.attrib[Tables.NAME]

            column_constraints_dict = {}

            for column in table[0]:
                column_name = column.attrib[Tables.NAME]
                column_data_type = column[0].text
                column_constraints = column[1].text

                column_properties = {
                    Tables.DATA_TYPE: column_data_type, Tables.CONSTRAINTS: column_constraints}

                column_constraints_dict[column_name] = column_properties

            self.table_columns_constraints_dict[table_name] = column_constraints_dict

    def get_table_names(self):
        return list(self.table_columns_constraints_dict.keys())

    def get_column_names(self, table_name):
        return list(self.table_columns_constraints_dict[table_name].keys())

    def get_insert_column_names(self, table_name):
        columns = self.table_columns_constraints_dict[table_name]
        return [column_name for column_name in columns if Tables.NOT_NULL in columns[column_name][Tables.CONSTRAINTS]]

    def get_column_constraints(self, table_name):
        columns = self.table_columns_constraints_dict[table_name]
        return [' '.join([column_name, columns[column_name][Tables.DATA_TYPE], columns[column_name][Tables.CONSTRAINTS]]) for column_name in columns]

    def get_create_table_commands(self):
        create_table_commands = []
        for table in self.table_columns_constraints_dict:
            columns = ', '.join(self.get_column_constraints(table))
            create_table_command = Tables.CREATE_TABLE_COMMAND.format(
                table, columns)
            create_table_commands.append(create_table_command)
        return(create_table_commands)

    def get_primary_key(self, table_name):
        columns = self.table_columns_constraints_dict[table_name]
        primary_key = [
            column_name for column_name in columns if Tables.PRIMARY_KEY in columns[column_name][Tables.CONSTRAINTS]][0]

        return primary_key
