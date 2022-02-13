import enum
from turtle import color

from database.postgresql_utils import PostgresqlUtils


class Letters(enum.Enum):
    letter_1 = 1
    letter_2 = 2
    letter_3 = 3
    letter_4 = 4
    letter_5 = 5


class QueryWords():
    TABLE_NAME = "Wordle_Words"
    WORD = "CONCAT(letter_1, letter_2, letter_3, letter_4, letter_5)"
    GREEN_QUERY_FORMAT = "{} = '{}'"
    YELLOW_QUERY_FORMAT = "{0} != '{1}' AND '{1}' IN ({2})"
    BLACK_QUERY_FORMAT = "'{}' NOT IN ({})"

    def __init__(self):
        self.postgresql_utilities = PostgresqlUtils()
        self.queries = {
            'g': self.green_query,
            'y': self.yellow_query,
            'b': self.black_query
        }

    def query(self, params):
        condition = 'WHERE ' + self.get_all_query(params)

        results = self.postgresql_utilities.get_rows(
            QueryWords.TABLE_NAME, QueryWords.WORD, condition)

        suggested_words = []

        for result in results:
            suggested_words.extend(result.values())

        self.print_suggestions(suggested_words)

    def print_suggestions(self, words):
        n = 5
        split_words = [words[i:i+n] for i in range(0, len(words), n)]
        for print_words in split_words:
            print('\t\t'.join(print_words))

    def green_query(self, position, letter):
        return QueryWords.GREEN_QUERY_FORMAT.format(Letters(position).name, letter)

    def yellow_query(self, position, letter):
        return QueryWords.YELLOW_QUERY_FORMAT.format(Letters(position).name, letter, self.get_compliment(position))

    def black_query(self, position, letter):
        return QueryWords.BLACK_QUERY_FORMAT.format(letter, self.get_all())

    def get_compliment(self, position):
        return ', '.join([Letters(x).name for x in range(1, 6) if x != position])

    def get_all(self):
        return ', '.join([Letters(x).name for x in range(1, 6)])

    def form_query(self, word, colors):
        formed_queries = []

        for (i, (letter, color)) in enumerate(zip(list(word.lower()), list(colors.lower()))):
            formed_queries.append('(' + self.queries[color](i+1, letter) + ')')

        return ' AND '.join(formed_queries)

    def get_all_query(self, params):
        all_queries = []
        for (word, colors) in params:
            all_queries.append('(' + self.form_query(word, colors) + ')')

        return ' AND '.join(all_queries)

    def get_input(self):
        inputs = []

        for i in range(6):
            word = input('Enter the word: ')
            color = input('Enter the colors: ')
            inputs.append((word, color))

            self.query(inputs)

            check = input('\n\nContinue (y/n): ')

            if check.lower() != 'y':
                break

            print('\n\n')


if __name__ == '__main__':
    query_words = QueryWords()

    query_words.get_input()
