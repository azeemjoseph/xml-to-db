import xml.etree.ElementTree as ET
from sqlalchemy import Float,create_engine, MetaData, Table, Column, Integer, String, Date, Numeric, ForeignKey, Index, ForeignKeyConstraint

# Define the path to your XML file
xml_file_path = "abc.xml"

# Define the database connection string
connection_string = "postgresql+psycopg2://user:password@localhost:5432/databasename"


# Create a database engine
engine = create_engine(connection_string)
metadata = MetaData()

# Parse the XML file
tree = ET.parse(xml_file_path)
root = tree.getroot()

# Dictionary to store table schemas
tables = {}

# Function to map XML types to SQLAlchemy types
def map_column_type(xml_type, size=None):
    if xml_type == 'INTEGER':
        return Integer
    elif xml_type == 'VARCHAR':
        # Ensure the size is an integer
        return String(int(size)) if size else String
    elif xml_type == 'TIMESTAMP':
        return String  # Use appropriate type for TIMESTAMP, adjust if needed
    elif xml_type == 'SMALLINT':
        return Integer  # SQLAlchemy maps SMALLINT to Integer
    elif xml_type == 'DATE':
        return Date
    elif xml_type == 'NUMERIC':
        return Numeric
    elif xml_type == 'BIT':
        return String  # Use a string type for BIT; adjust if needed
    elif xml_type == 'DOUBLE':
        return Float
    else:
        raise ValueError(f"Unknown type: {xml_type}")


# Extract table and column information
for table_element in root.findall('table'):
    table_name = table_element.get('name')
    columns = []
    indexes = []
    constraints = []

    # Extract columns
    for column_element in table_element.findall('column'):
        col_name = column_element.get('name')
        col_type = column_element.get('type')
        col_size = column_element.get('size')
        col_default = column_element.get('default')
        col_required = column_element.get('required') == 'true'
        col_primary_key = column_element.get('primaryKey') == 'true'
        
        sql_type = map_column_type(col_type, col_size)
        
        # Initialize foreign keys set for each column
        foreign_keys = set()
        
        column = Column(
            col_name,
            sql_type,
            primary_key=col_primary_key,
            nullable=not col_required,  # nullable=True if col_required=False
            default=col_default
        )
        
        # Add to columns list
        columns.append(column)
        
    # Extract indexes
    for index_element in table_element.findall('index'):
        index_name = index_element.get('name')
        index_columns = [index_column.get('name') for index_column in index_element.findall('index-column')]
        indexes.append(Index(index_name, *index_columns))
    
    # Extract foreign keys and add them directly in Column definitions
    for foreign_key_element in table_element.findall('foreign-key'):
        foreign_table = foreign_key_element.get('foreignTable')
        references = foreign_key_element.findall('reference')
        for reference in references:
            local_col = reference.get('local')
            foreign_col = reference.get('foreign')
            
            # Update existing columns to add ForeignKey constraint
            for col in columns:
                if col.name == local_col:
                    constraints.append(ForeignKeyConstraint([col.name], [f'{foreign_table}.{foreign_col}']))

    # Create table object with columns, indexes, and constraints
    table = Table(table_name, metadata, *columns, *indexes, *constraints)
    tables[table_name] = table

# Create all tables in the database
metadata.create_all(engine)

print("Database schema created successfully.")
