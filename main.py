import typer
from DataAccess.DataAccess import DataAccess
from FileMaker.DataObject import DataObject

app = typer.Typer()

@app.command()
def hello(name: str):
    print(f"Hello {name}")
    
@app.command()
def goodbye(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


'''

python main.py generateobjects root CvyQqGGS2XSatrYBrT 103.252.117.180 3306 ToastlyDev CouncilApp

'''


@app.command()
def generateObjects(username: str, 
                    password: str, 
                    host: str, 
                    port: str, 
                    database: str,
                    destination: str): 
                    
    data_access = DataAccess(Username = username, 
                             Password = password, 
                             Host = host, 
                             Port = port, 
                             Database = database)
    
    # Connect to Database
    cursor = data_access.raw_connection.cursor()
    cursor.execute("""SELECT TABLE_NAME FROM information_schema.tables
                      WHERE table_schema = '""" + database + """'
                      ORDER BY table_name;""")

    # Return all tables
    tables = cursor.fetchall()

    # For each table -
    for table in tables:
        table_name = table[0]
        cursor.execute("""SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, COLUMN_KEY, IS_NULLABLE FROM information_schema.columns
                          WHERE table_schema = '""" + database + """'
                             AND table_name = '""" + table_name + """' ORDER BY ORDINAL_POSITION;""")

        # 1.) Get all fields with their metadata,
        columns = cursor.fetchall()

        data_object = DataObject(Name = table_name, Columns = columns, Destination = destination) 

        #for column in columns:
        #    print(" *********  : " + column[3])

        # 2.) Create data objects ()
    
    cursor.close()
    print("Objects generated")

if __name__ == "__main__":
    app()