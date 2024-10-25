import mysql.connector
from decimal import Decimal

destination_config = {
                            'host':'localhost',
                            'database':'testdb',
                            'user':'root',
                            'password':'#2002#'
}

def get_connection():
    db = mysql.connector.connect(
        host='localhost',
        database='practice',
        user='root',
        password='#2002#'
    )
    return db


def get_table_from_db():

    try:
         mydb = get_connection()
         cursor = mydb.cursor()
         cursor.execute("select * from emp")
         records = cursor.fetchall()
         columns = [column[0] for column in cursor.description]
         cursor.close()
         mydb.close()
         print('data extracted from emp table')
         return records,columns
    except mysql.connector.Error as e:
        print("error extracting data",e)


def transforming_to_float(data):
    trans= [
        tuple(float(value) if isinstance(value,Decimal) else value for value in row)    # converting decimal to float
        for row in data
    ]
    print("transformer")
    return trans

def load_data(data,columns):
    dest_conn = mysql.connector.connect(**destination_config)
    cursor= dest_conn.cursor()

    # ill create table here itself
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS emp (
            emp_id INT ,                  
            ename VARCHAR(255),
            dept_name VARCHAR(100),
            salary FLOAT
        )
        """
    cursor.execute(create_table_query)

    columns_to_string = ', '.join(columns)       # join gets u string, col1, col2, col3
    placeholder = ', '.join(['%s']*len(columns))
    insert_q = f'insert into emp ({columns_to_string}) values ({placeholder})'

    cursor.executemany(insert_q,data)
    dest_conn.commit()
    cursor.close()
    dest_conn.close()
    print("loaded to destination")

def etl_run():
    records,columns= get_table_from_db()
    trans_data= transforming_to_float(records)
    load_data(trans_data,columns)


etl_run()


# joins = ', '.join('%s'*3)                       stooopid
# print(joins)