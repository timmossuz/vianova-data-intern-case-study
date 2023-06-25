import pandas as pd
import sqlite3
import requests
import os



csv_url = 'https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B'
csv_filename = 'geonames.csv'

# Check if the file already exists
if os.path.exists(csv_filename):
    print(f'{csv_filename} already exists. Skipping download.')
else:
    # Send a GET request to the CSV file URL
    response = requests.get(csv_url)
    
    if response.status_code == 200:
        with open(csv_filename, 'wb') as file:
            file.write(response.content)
        print(f'{csv_filename} downloaded successfully.')
    else:
        print(f'Failed to download {csv_filename}.')

# Continue with reading the CSV file using pandas
df = pd.read_csv(csv_filename, delimiter=';')





# Group the data by Country Code and Country Name EN
grouped = df.groupby(['Country Code', 'Country name EN'])

# Calculate the maximum population for each country
max_pop = grouped['Population'].max().reset_index()

# Rename the columns
max_pop.columns = ['Country Code', 'Country Name EN', 'maxPop']

# Display the resulting dataset
print(max_pop)



# Establish a connection to the SQLite database
conn = sqlite3.connect('geonames.db')

# Create a table in the database and insert the DataFrame data
max_pop.to_sql('countries', conn, if_exists='replace', index=False)

# Execute an SQL query to select countries with maxPop less than 10,000,000
query = "SELECT [Country Code], [Country Name EN] FROM countries WHERE maxPop < 10000000 ORDER BY [Country Name EN]"

# Fetch the query result into a pandas DataFrame
result = pd.read_sql_query(query, conn)

# Save the result as a tab-separated value (TSV) file
result.to_csv('countries_filtered.tsv', sep='\t', index=False)

# Close the database connection
conn.close()







