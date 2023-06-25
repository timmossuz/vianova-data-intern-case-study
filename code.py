import pandas as pd
import sqlite3


df = pd.read_csv('geonames.csv', delimiter=';')
print(df.head())



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







