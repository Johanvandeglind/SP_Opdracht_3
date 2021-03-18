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
    for record in records:
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
        sql.append((record[0], record[1], record[3], types, record[8], record[9], record[10], doelgroep, record[2],deal, record[6],record[13]))
    print(f"recommendation_{value} table created/filled")
    cur.executemany(f"INSERT INTO recommendation_{value} VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",sql)
    con.commit()


create_content_recommendation_table('sub_sub_category', 'Deodorant', 'selling_price')
#create_content_recommendation_table('brand','Nivea','selling_price')

def get_highest_key(array:list):
    '''
    Returns meest voorkomende naam in array
    :param array: de ingegeven list
    :return: Meest gekozen
    '''
    dict_count ={}
    for array in array:
        if array not in dict_count:
            dict_count[array] = 1
        elif array in dict_count:
            dict_count[array] += 1
    return max(dict_count, key=dict_count.get)

def get_most_used_bases(profile_id:str,base_1:str,base_2:str):
    cur.execute(
        f"SELECT product_id FROM orders inner join buids on orders.sessionsid = buids.sessionsid where(buids.profileprofile_id = '{profile_id}') ")
    # cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profil_id}') ")
    records = cur.fetchall()
    viewed_products = []
    for record in records:
        viewed_products.append(record[0])
    cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profile_id}') ")
    records = cur.fetchall()
    for record in records:
        viewed_products.append(record[0])
    lst_base_1 = []
    lst_base_2 = []
    for productid in viewed_products:
        cur.execute(f"SELECT {base_1},{base_2} FROM product where(id = '{productid}') ")
        records = cur.fetchall()
        lst_base_1.append(records[0][1])
        lst_base_2.append(records[0][0])

    if len(lst_base_1) > 0:
        high_base_1 = get_highest_key(lst_base_1)
    else:
        high_base_1 = None
    if len(lst_base_2) > 0:
        high_base_2 = get_highest_key(lst_base_2)
    else:
        high_base_2 = None
    return high_base_1,high_base_2


def create_collaborative_recommendationstables_V2(base_1:str,base_2:str):
    sql = []
    cur.execute("""CREATE TABLE IF NOT EXISTS collaborative_recommendations
                    (%s VARCHAR,
                     %s VARCHAR,
                     lst_profile_id VARCHAR);"""%(base_1,base_2))
    con.commit()
    cur.execute("DELETE FROM collaborative_recommendations")
    cur.execute(f"SELECT profile_id FROM profile ")
    records = cur.fetchall()
    combination ={}
    for profile_id in records:
        high_base_1,high_base_2 = get_most_used_bases(profile_id[0],base_1,base_2)
        if (high_base_1,high_base_2) not in combination and high_base_1!=None and high_base_2!=None:
            combination[(high_base_1, high_base_2)] = []
            combination[(high_base_1,high_base_2)].append(profile_id[0])
        elif (high_base_1,high_base_2) in combination and high_base_1!=None and high_base_2!=None:
            combination[(high_base_1,high_base_2)].append(profile_id[0])
    sql=[]
    for key,value in combination.items():
        sql.append((key[0],key[1],str(value)))
    cur.executemany("INSERT INTO collaborative_recommendations VALUES (%s,%s,%s);",sql)
    con.commit()



# create_collaborative_recommendation_table('5a393d68ed295900010384ca','5a393d68ed295900010384ca')
dict = create_collaborative_recommendationstables_V2('sub_category','gender')
cur.close()
con.close()
