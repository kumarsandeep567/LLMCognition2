import sqlalchemy as db

# MySQL connection details
DB_USER = "root"
DB_PASSWORD = ""  # Empty password
DB_HOST = "localhost"
DB_NAME = "gaia_database"

# Create an engine to connect to the MySQL database
engine = db.create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

# Establish a connection
connection = engine.connect()

# Metadata to reflect the database structure
metadata = db.MetaData()

# Load the table structure (assuming your table is named 'analytics')
analytics = db.Table('analytics', metadata, autoload_with=engine)

# Example query: Fetch all data from the 'analytics' table
query = db.select([analytics])
result = connection.execute(query).fetchall()

# Print the results for verification
for row in result:
    print(row)

# Don't forget to close the connection after use
connection.close() 