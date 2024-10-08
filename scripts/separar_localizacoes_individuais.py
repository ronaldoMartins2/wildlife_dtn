#Código-fonte 1 – Script em python para obter as localizações individuais

import csv
import sys
from datetime import datetime as dt

current_animal = sys . argv [1]

# create the csv writer
file = open ( 'map_'+ current_animal + '.csv', 'w')

fields = ( 'id', 'time', 'long', 'lat')
writer = csv . DictWriter ( file, fieldnames = fields, lineterminator= '\ n ')

# empty dictionary
list_animals = { }

actualTimeEvenStr = '1/1/2022 17:39'
actualTimeEven = dt . strptime ( actualTimeEvenStr, "%m/%d/%Y %H:%M" )

with open ( 'jaguar_mamiraua.csv') as csv_file :
    csv_reader = csv . reader ( csv_file, delimiter = ',')
    line_count = 0

    for row in csv_reader :

        if line_count == 0:
            print ( f' Column names are { " , ". join ( row ) } ')
            line_count += 1
        else :
            currentTime = dt . strptime ( row [ 1 ] , "%m/%d/%y %H:%M" )

            if currentTime < actualTimeEven :
                actualTimeEven = currentTime

            list_animals [ row[6 ] ] = ' id '

            line_count += 1

    print ( f' Processed {line_count} lines . ')


print ( ' firstDate : ')
print ( actualTimeEven )

with open ( 'jaguar_mamiraua.csv') as csv_file :
    csv_reader = csv.reader( csv_file, delimiter = ',')
    line_count = 0
    
    for row in csv_reader :
        if line_count == 0:
            print ( f' Column names are { ",". join ( row ) } ')
            line_count += 1
        else :

            list_animals [ row[6] ] = 'id'

            if row [ 6 ] == current_animal :
                writer.writerow ( { 'id': row[6], 'time': row[1], 'long': row[2], 'lat': row[3] } )

            line_count += 1
    print ( f' Processed {line_count} lines . ')

# close the file
file.close( )