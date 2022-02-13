import wget


url = 'https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt'

filename = wget.download(url)

with open(filename) as file:
    words = file.readlines()


with open("5_letter_words.txt", "w") as output:
    for word in words:
        if len(word.strip()) == 5:
            output.write(word)
