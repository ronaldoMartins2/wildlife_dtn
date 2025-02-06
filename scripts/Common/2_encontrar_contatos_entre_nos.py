import csv
from datetime import datetime as dt
import geopy.distance
import sys
import psycopg2

# python3 2_encontrar_contatos_entre_nos.py 93 94

first_animal = sys.argv[1]
second_animal = sys.argv[2]

# distancia total considerada como contato entre dois animais em km
distanceGoal = 10
# n ú mero de linhas des registros dos dados brutos
num_lines_to_analyse = 6900

coords_manaus = (-3.1190, -60.0217)

# lat and log Manaus
fix_longitude = -60.0217
fix_latitude = -3.1190

# open the file in the write mode
first = 'map_'+ first_animal + '.csv'

second = 'map_'+ second_animal + '.csv'

file = open ( 'contact_'+ first_animal + '_'+ second_animal + '.csv', 'w')

# create the csv writer
fields = ( 'id', 'conn', 'for', 'to', 'state')
writer = csv . DictWriter ( file, fieldnames = fields, lineterminator= '\n')

# empty dictionary
list_animals = { }

state = 'down'

firstTimeEvenStr = '12/11/2010 13:01'
firstTimeEven = dt.strptime ( firstTimeEvenStr, "%m/%d/%Y %H:%M" )
# firstTimeEven = '3/12/2014 17:39'

# database : dtn_contacts
conn = psycopg2.connect (
                            host = "localhost" ,
                            database = "dtn_contacts" ,
                            port = "5432" ,
                            user = "postgres" ,
                            password = "postgres" )

print ( 'conn to database')
print ( conn )

# create a cursor
cur = conn . cursor ()

# execute a statement
print ( 'PostgreSQL database version : ')
cur . execute ( ' SELECT version () ')

# display the PostgreSQL database server version
db_version = cur . fetchone ()
print ( db_version )

def insert_contact ( id, conn_contact, for_contact, to_contact, state ) :
# insert a new jaguar contact into the jaguar_contacts table 

    sql = """ INSERT INTO jaguar_contacts ( id, conn, for_contact,
                                            to_contact, state )
                                            VALUES (%s, %s, %s, %s, %s ) RETURNING id ; """

    contact_id = None

    try :

        print ( 'id :'+ str(id) + ' conn_contact ' + conn_contact +' for_contact : ' + for_contact + ' to : ' + to_contact + ' state :' + state )

        # execute the INSERT statement

        cur . execute ( sql, ( float ( id ) , conn_contact, int ( for_contact ) , int ( to_contact ) , state ) )
        cur . execute ( sql, (0 , 'CONN', 0 , 1 , 'up') )


        # get the generated id back
        contact_id = cur . fetchone () [ 0 ]

    except ( Exception, psycopg2.DatabaseError ) as error :
        print ( error )

    return contact_id

coords_2 = ( fix_latitude, fix_longitude )

with open ( first ) as csv_file :
    csv_reader = csv . reader ( csv_file, delimiter = ',')
    line_count = 0

    print('inside with')

    for row in csv_reader :

        print('inside for in csv_reader')

        # ’%Y -% m -% d % H :% M :% S .% f ’
        timeFirst = dt . strptime ( row [1] , "%m/%d/%y %H:%M" )

        if '.' in  row[3] and '.' in row[2]:
            coords_1 = ( row[2] , row [3] )
        else:
            continue

        with open ( second ) as csv_file_second :

            print('inside with in csv_file_second')

            csv_reader_second = csv . reader ( csv_file_second, delimiter = ',')
            line_count2 = 0

            for row_2 in csv_reader_second :
                print('inside row_2 in csv_reader_second')

                timeSecond = dt . strptime ( row_2[1] , "%m/%d/%y %H:%M" )
                
                if '.' in  row_2[3] and '.' in row_2[2]:
                    coords_2 = ( row_2[2] , row_2 [3] )
                else:
                    continue

                # calcula a diferen ç a entre o tempo inicial e o tempo corrente
                difference = timeFirst - firstTimeEven
                difference_in_s = difference . total_seconds ()
                # transforma a diferen ç a de segundos para minutos
                minutes = divmod ( difference_in_s, 60) [ 0 ]
                # hours = divmod ( difference_in_s, 3600) [ 0 ]

                # verifica se os animais se encontram na mesma hora

                print(f'inside if timeFirst == timeSecond {timeFirst} = {timeSecond}')

                if timeFirst == timeSecond :
                    print(f'inside if timeFirst == timeSecond coords_1 {coords_1} coords_2 {coords_2}')

                    distance = geopy.distance.geodesic(coords_1, coords_2 ).km
                    print ( 'distance : ')
                    print ( distance )

                    if distance < distanceGoal :
                        # 'id', 'conn', 'for', 'to', 'state'
                        writer . writerow ( { 'id': minutes, 'conn': 'CONN', 'for': first_animal, 'to': second_animal, 'state': 'up'} )
                        state = 'up'
                    else:
                        print( f'distancia maior que {distanceGoal} km ')

                line_count2 += 1

        line_count += 1
        # 6912 numero de registros gerais
        if line_count == num_lines_to_analyse :
            break
    
    print ( f'Processed {line_count} lines.')
           
# close the file
file.close()

# commit the changes to the database
conn.commit()

# close the communication with the PostgreSQL
cur.close()