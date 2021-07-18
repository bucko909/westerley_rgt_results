import lxml.etree, lxml.html
import os.path
import requests
import csv

def get_scoreboard_dave():
    csvfile = open(f'data/countries-in.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if len(row) != 3:
            raise Exception(row)
        yield row

def get_scoreboard_me():
    csvfile = open(f'out/user_results.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    i = iter(csvreader)
    next(i)
    for row in i:
        yield row[1:3] + [row[4]]

dave_by_score = {}
for name, country, score in get_scoreboard_dave():
    dave_by_score.setdefault(score, [])
    if country == '???':
        country = None
    dave_by_score[score].append((name, country))

csvfile = open('data/countries.csv', 'w')
csvwriter = csv.writer(csvfile)
for url, name, score in get_scoreboard_me():
    name = name.replace('  ', ' ')
    dave_possibilities = dave_by_score[score]
    if len(dave_possibilities) == 1:
        del dave_by_score[score]
        csvwriter.writerow((url, name) + dave_possibilities[0])
        continue
    filtered = [(dave_name, country) for (dave_name, country) in dave_possibilities if dave_name.startswith(name) or name.startswith(dave_name) or (' ' in name and ' ' in dave_name and name.split(' ')[1] == dave_name.split(' ')[1])]
    if len(filtered) == 1:
        dave_by_score[score] = [(dave_name, country) for dave_name, country in dave_possibilities if dave_name != filtered[0][0]]
        csvwriter.writerow((url, name) + filtered[0])
        continue
    print("Could not locate", url, name, score)
    csvwriter.writerow((url, name, None, None))

print("Remaining", dave_by_score)
