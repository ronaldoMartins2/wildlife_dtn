import csv
import sys
from datetime import datetime as dt
import psycopg2
from random import randrange

# python3 3_adicionar_evento_down.py contact_93_97.csv

fileTarget = sys.argv [1]


# create the csv writer
# writer = csv . writer ( f )
file = open ( 'down_'+ fileTarget + '.csv', 'w')

fields = ( 'id', 'conn', 'for', 'to', 'state')
writer = csv . DictWriter ( file, fieldnames = fields, lineterminator= '\n')

# database : dtn_contacts
conn = psycopg2 . connect (
                            host = "localhost" ,
                            database = "dtn_contacts" ,
                            port = "5432" ,
                            user = "postgres" ,
                            password = "postgres" )

print ( 'conn to database')
print ( conn )

# create a cursor
cur = conn.cursor ()

# execute a statement
print ( 'PostgreSQL database version : ')
cur . execute ( ' SELECT version () ')

# display the PostgreSQL database server version
db_version = cur.fetchone ()
print ( db_version )

def convert_contact_number ( contact_number ) :
    return int ( contact_number ) -93

def insert_contact ( id, conn_contact, for_contact, to_contact, state ) :
    """ insert a new jaguar contact into the jaguar_contacts table """

    sql = """ INSERT INTO jaguar_contacts ( id, conn, for_contact, to_contact, state )
            VALUES (%s, %s, %s, %s, %s ) RETURNING id ; """
    
    # conn = None
    contact_id = None
    try :

        # execute the INSERT statement

        cur.execute ( sql, ( float ( id ) , conn_contact, int (for_contact ) , int ( to_contact ) , state ) )
        # cur . execute ( sql, (0 , ’ CONN ’, 0 , 1 , ’ up ’) )

        # get the generated id back
        contact_id = cur . fetchone () [ 0 ]

    except ( Exception, psycopg2 . DatabaseError ) as error :
        print ( error )

        return contact_id

with open ( fileTarget + '.csv') as csv_file :
    csv_reader = csv . reader ( csv_file, delimiter = ',')
    line_count = 0

    for row in csv_reader :

        hour = float ( row [ 0 ] ) + randrange (5)
        writer . writerow ( { 'id': hour, 'conn': row[1], 'for': row[2], 'to': row[3], 'state': row[4] } )
        # writer . writerow ( { ’ id ’:( hour +1) , ’ conn ’: row[1], ’ for ’: row[2], ’ to ’: row[3], ’ state ’: ’ down ’} )
        writer . writerow ( { 'id':( hour +60) , 'conn': row[1], 'for':row[2], 'to': row[3], 'state': 'down'} )

        insert_contact( hour, row[1] , str ( convert_contact_number( row[2]) ) , str ( convert_contact_number (row[3]) ) , row[4] )
        # insert_contact (( hour +1) , row [ 1 ] , str (convert_contact_number ( row [ 2 ] ) ) , str ( convert_contact_number (row [ 3 ] ) ) , ’ down ’)
        insert_contact(( hour +60) , row [1] , str (convert_contact_number (row [2]) ) , str ( convert_contact_number (row[3]) ) , 'down')

        line_count += 1
        print ( f'Processed {line_count} lines . ')

# close the file
file . close ()

# commit the changes to the database
conn . commit ()

# close the communication with the PostgreSQL
cur.close ()