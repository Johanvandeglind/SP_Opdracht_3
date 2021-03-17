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


def create_content_recommendation_table(colmn, value, order):
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
    cur.execute(f"SELECT * FROM product  WHERE ({colmn} = '{value}' and selling_price > 0 and stock_level > 0)  ORDER BY {order} ASC limit 4 ")  # % (table,colmn,value,))
    records = cur.fetchall()
    #print(records)
    product = []
    for record in records:
        #print(record)
        cur.execute(f"SELECT * FROM properties WHERE (productid = '{record[0]}') ORDER BY productid ASC")
        properties = cur.fetchall()
        deal = None
        types =  None
        doelgroep =None
        for prop in properties:
            if prop[1] == 'discount':
                deal = prop[2]
            if prop[1] =='soort':
                types = prop[2]
            if prop[1] == 'doelgroep':
                doelgroep = prop[2]
    #
        sql.append((record[0], record[1], record[3], types, record[8], record[9], record[10], doelgroep, record[2],deal, record[6],record[13]))
    print(f"recommendation_{value} table created/filled")
    cur.executemany(f"INSERT INTO recommendation_{value} VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",sql)
    con.commit()


create_content_recommendation_table('sub_sub_category', 'Deodorant', 'selling_price')
#create_content_recommendation_table('brand','Nivea','selling_price')

def create_collaborative_recommendation_table(profil_id, value):
    sql = []
    # cur.execute("""CREATE TABLE IF NOT EXISTS recommendation_%s
    #                 (id VARCHAR PRIMARY KEY,
    #                  name VARCHAR,
    #                  brand VARCHAR,
    #                  type VARCHAR,
    #                  category VARCHAR,
    #                  subcategory VARCHAR,
    #                  subsubcategory VARCHAR,
    #                  targetaudience VARCHAR,
    #                  selling_price INTEGER,
    #                  deal VARCHAR,
    #                  description VARCHAR,
    #                  stock INTEGER );""" % (value))
    # con.commit()
    # cur.execute("DELETE FROM recommendation_%s"% (value))

    cur.execute(f"SELECT product_id FROM orders inner join buids on orders.sessionsid = buids.sessionsid where(buids.profileprofile_id = '{profil_id}') ")
    #cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profil_id}') ")
    records = cur.fetchall()

    for record in records:
        print (record)
    print("--------------------------------------")
    cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profil_id}') ")
    records = cur.fetchall()

    for record in records:
        print(record)

create_collaborative_recommendation_table('5a393d68ed295900010384ca','5a393d68ed295900010384ca')
cur.close()
con.close()
