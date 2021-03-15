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


def create_content_recommendation_table(table, colmn, value, order):
    sql = []
    cur.execute("""CREATE TABLE IF NOT EXISTS recommendation_%s 
                    (id VARCHAR PRIMARY KEY,
                     name VARCHAR,
                     brand VARCHAR,
                     type VARCHAR,
                     category VARCHAR,
                     subcategory VARCHAR,
                     subsubcategory VARCHAR,
                     targetaudience VARCHAR,
                     selling_price INTEGER,
                     deal VARCHAR,
                     description VARCHAR,
                     stock INTEGER );""" % (value))
    con.commit()
    cur.execute("DELETE FROM recommendation_%s"% (value))
    cur.execute(f"SELECT * FROM {table} WHERE ({colmn} = '{value}' and selling_price > 0 and stock_level > 0) ORDER BY {order} ASC LIMIT 4 ")  # % (table,colmn,value,))
    records = cur.fetchall()
    product = []
    for record in records:
        #print(record)
        cur.execute(f"SELECT * FROM properties WHERE (productid = '{record[0]}') ORDER BY productid ASC")  # % (table,colmn,value,))
        properties = cur.fetchall()
        #print(properties)
        deal =None
        types = None
        doelgroep = None
        for prop in properties:
            #print(prop[1])
            if prop[1] == 'discount':
                deal = prop[2]
            if prop[1] =='soort':
                types = prop[2]
            if prop[1] == 'doelgroep':
                doelgroep = prop[2]

        sql.append((record[0], record[1], record[3], types, record[8], record[9], record[10], doelgroep, record[2],deal, record[6],record[13]))
        print(properties)
    cur.executemany(f"INSERT INTO recommendation_{value} VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",sql)
    con.commit()


create_content_recommendation_table('product', 'sub_sub_category', 'Deodorant', 'selling_price')
create_content_recommendation_table('product','brand','Nivea','selling_price')


cur.close()
con.close()
