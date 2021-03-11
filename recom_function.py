import datetime
import psycopg2

time0 = datetime.datetime.now()


con = psycopg2.connect(
    host="localhost",
    database="opdracht2_final",
    user="postgres",
    password="kip12345",
)
cur = con.cursor()

def create_content_recommendation_table(table,colmn,value,order):

    cur.execute("""CREATE TABLE IF NOT EXISTS recommendation_%s 
                    (id VARCHAR PRIMARY KEY,
                     name VARCHAR,
                     brand VARCHAR,
                     type VARCHAR,
                     category VARCHAR,
                     subcategory VARCHAR,
                     subsubcategory VARCHAR,
                     targetaudience VARCHAR,
                     discount INTEGER,
                     sellingprice INTEGER,
                     deal VARCHAR,
                     description VARCHAR);"""%(colmn))
    con.commit()

    cur.execute(f"SELECT * FROM {table} WHERE ({colmn} = '{value}') ORDER BY {order} ASC LIMIT 4 ") #% (table,colmn,value,))
    records = cur.fetchall()
    for record in records:
        print(record)

create_content_recommendation_table('product','sub_sub_category','Deodorant','selling_price')
create_content_recommendation_table('product','brand','Nivea','selling_price')


cur.close()
con.close()