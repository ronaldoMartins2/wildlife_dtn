#Código-fonte 1 – Script em python para obter as localizações individuais

# para executar
# python3 1_separar_localizacoes_individuais.py 94

import csv
import sys
from datetime import datetime as dt
import pandas as pd
from shapely.geometry import Point
from geopandas import GeoDataFrame
import geodatasets
import geopandas as gpd
import matplotlib.pyplot as plt

current_animal = sys.argv [1]

# create the csv writer
file = open ( 'map_'+ current_animal + '_plot.csv', 'w')

fields = ( 'lat', 'long')
writer = csv . DictWriter ( file, fieldnames = fields, lineterminator= '\n')

# empty dictionary
list_animals = { }

actualTimeEvenStr = '1/1/2022 17:39'
actualTimeEven = dt . strptime ( actualTimeEvenStr, "%m/%d/%Y %H:%M" )

file_rawdata = 'jaguar_mamiraua.csv'

custom_header = ['lat', 'lng']
df = pd.DataFrame(columns=custom_header)

with open ( file_rawdata ) as csv_file :

    print('inside main with')

    csv_reader = csv . reader ( csv_file, delimiter = ',')
    line_count = 0

    for row in csv_reader :

        if line_count == 0:
            print ( f' Column names are (first with) { ",". join ( row ) } ')
            line_count += 1
        else :
            currentTime = dt . strptime ( row[1] , "%m/%d/%y %H:%M" )

            if currentTime < actualTimeEven :
                actualTimeEven = currentTime

            list_animals [ row[6] ] = 'id'

            line_count += 1

    print ( f' Processed {line_count} lines. list_animals {len(list_animals)}')

print( f'list_animals {list_animals}' )

print ( ' firstDate : ')
print ( actualTimeEven )

data = []

with open ( file_rawdata ) as csv_file :
    csv_reader = csv.reader( csv_file, delimiter = ',')
    line_count = 0
    
    for row in csv_reader :
        if line_count == 0:
            
            print ( f' Column names are { ",". join ( row ) } ')
            #writer.writerow ( { 'lat': 'lat', 'long': 'lng' } )

        else :

            list_animals [ row[6] ] = 'id'

            if row[6] == current_animal :
                
                #writer.writerow ( { 'lat': row[3], 'long': row[2] } )

                #row_subset = row[3] + row[2]

                #remove zero coordinators
                if '.' in row[2] and '.' in row[3] :
                    data.append( [ row[2], row[3]] )
                else:
                    continue

                # Create a DataFrame from the current row
                #temp_df = pd.DataFrame([row_subset], columns=custom_header)
                #temp_df = pd.DataFrame(row_subset, columns=custom_header)

                # Append the temporary DataFrame to the main DataFrame
                #df = pd.concat([df, temp_df], ignore_index=True)
                
        line_count += 1

    print ( f' Processed {line_count} lines. ')

# close the file
file.close( )

#print(f'data {data}')
#sub_element = [ data[0] ]
#sub_element = data[:1000]

#print(f'sub_element {sub_element}')

#df = pd.DataFrame(sub_element, columns=custom_header)
df = pd.DataFrame(data, columns=custom_header)

geometry = [Point(xy) for xy in zip(df['lat'], df['lng'])]

#print(f'geometry {geometry}')

gdf_data = GeoDataFrame(df, geometry=geometry)   

#print(f'gdf {gdf}')

#this is a simple map that goes with geopandas
# deprecated: world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
#world = gpd.read_file(geodatasets.data.naturalearth.land['url'])
#world = gpd.read_file(geodatasets.get_path('naturalearth.land'))

#print(f' world.columns {world.columns}')


# Plot
fig, ax = plt.subplots(figsize=(10, 6))
gdf_data.plot(ax=ax, color='lightgrey')  # Plot all geometries
#amazonas.plot(ax=ax, color='green')   # Highlight Amazonas state


plt.title(f'Geographic Distributions of onça locomation #{current_animal}')
plt.xlabel('Longitude')
plt.ylabel('Latitude')

# Save the plot as an image
plt.savefig(f'onca_{current_animal}.png')

plt.show()
