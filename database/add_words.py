from database.postgresql_utils import PostgresqlUtils


class AddWords():
    TABLE_NAME = 'Wordle_Words'
    INSERT_FORMAT = "'{}', '{}', '{}', '{}', '{}'"

    def __init__(self):
        self.postgresql_utilities = PostgresqlUtils()
        with open('database/data/5_letter_words.txt') as file:
            self.words = file.readlines()

    def index_words(self):

        for word in self.words:
            self.index_word(word)

    def index_word(self, word):
        row = self.postgresql_utilities.insert_values(
            AddWords.TABLE_NAME, self.get_values(word))

    def get_values(self, word):
        return AddWords.INSERT_FORMAT.format(*list(word))


if __name__ == '__main__':
    add_words = AddWords()

    add_words.index_words()
