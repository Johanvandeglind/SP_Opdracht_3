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

t0 = datetime.datetime.now()
def create_content_recommendation_table(colmn, value, order):
    """
    This function creates a table with 4 recommended items that ar the outcome of the inputted parameters.
    :param colmn: The colm witch the recommendation is based on, so for example sub_category
    :param value: The value this colmn had to have, for brand this can be nivea and for sub_sub_category this can be deoderant
    :param order: The order the products are filtered i, this an be on ID or on selling_price or in every toher colmn name
    :return: A created table in pgadmin
    """
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


#create_content_recommendation_table('sub_sub_category', 'Deodorant', 'selling_price')
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
    """

    This funciton scans the given profile for all the viewd and ordered products and gets the inputted colmn values from these products.
    These valuas are called base_1 and base_2,
    for example if i chose gender and sub_category,
    the return wil provide the most chosen gender and the most chosen sub_category of this profile.
    :param profile_id: The profile we want to retrive data from
    :param base_1: The first colmn name
    :param base_2: The second colmn name
    :return: the most chosen base_1 and the most chosen base_2
    """
    # cur.execute(
    #     f"SELECT product_id FROM orders inner join buids on orders.sessionsid = buids.sessionsid where(buids.profileprofile_id = '{profile_id}') ")
    # cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profil_id}') ")
    #records = cur.fetchall()
    #viewed_products = []
    # for record in records:
    #     viewed_products.append(record[0])
    #cur.execute(f"SELECT productid FROM viewed_before where(profileprofile_id = '{profile_id}') ")
    #records = cur.fetchall()
    lst_base_1 = []
    lst_base_2 = []

    cur.execute(f"SELECT {base_1},{base_2} FROM product inner join viewed_before on viewed_before.productid = product.id where(viewed_before.profileprofile_id = '{profile_id}') ")
    records = cur.fetchall()
    #print(records)
    for prod in records:
        if len(prod) != 0:
            lst_base_1.append(prod[1])
            lst_base_2.append(prod[0])

    if len(lst_base_1) != 0:
        high_base_1 = get_highest_key(lst_base_1)
    else:
        high_base_1 = None
    if len(lst_base_2) != 0:
        high_base_2 = get_highest_key(lst_base_2)
    else:
        high_base_2 = None
    #print(high_base_1,high_base_2)
    # high_base_1 = 'hgello'
    # high_base_2 = 'haai'
    return high_base_1,high_base_2


def create_collaborative_recommendationstables_V2(base_1:str,base_2:str):
    '''
    This function generates a table with all the possible combinations of the inputed colmn values
    These valuas are called base_1 and base_2,
    for example if i chose gender and sub_category,
    The function wil generate all possible combinations of values and with this combination the profile that match these references

    :param base_1: The first colmn name
    :param base_2: The second colmn name
    :return: A filled table with all combinations and profile ID's that mach them
    '''
    print('function create_collaborative_recommendationstables_V2 started')
    cur.execute("""CREATE TABLE IF NOT EXISTS collaborative_recommendations
                    (%s VARCHAR,
                     %s VARCHAR,
                     lst_profile_id VARCHAR);"""%(base_1,base_2))
    con.commit()
    cur.execute("DELETE FROM collaborative_recommendations")
    con.commit()
    cur.execute(f"SELECT profile_id FROM profile limit 100")
    records = cur.fetchall()
    combination ={}
    i= 0
    for profile_id in records:
        #print(i,datetime.datetime.now() - time0)
        i+=1
        #print(profile_id)
        high_base_1,high_base_2 = get_most_used_bases(profile_id[0],base_1,base_2)
        if (high_base_1,high_base_2) not in combination and high_base_1!=None and high_base_2!=None:
            combination[(high_base_1, high_base_2)] = []
            combination[(high_base_1,high_base_2)].append(profile_id[0])
        elif (high_base_1,high_base_2) in combination and high_base_1!=None and high_base_2!=None:
            combination[(high_base_1,high_base_2)].append(profile_id[0])
        if i % 100 == 0:
            print(i,datetime.datetime.now() - time0)
    sql=[]
    for key,value in combination.items():
        sql.append((key[0],key[1],str(value)))
    print(datetime.datetime.now() - time0,'before executemany')
    cur.executemany("INSERT INTO collaborative_recommendations VALUES (%s,%s,%s);",sql)
    con.commit()
    print(datetime.datetime.now() - time0,'after executemany')


# create_collaborative_recommendation_table('5a393d68ed295900010384ca','5a393d68ed295900010384ca')
#create_collaborative_recommendationstables_V2('sub_category','gender')
def test(base_1:str,base_2:str,record:list):
    cur.execute(f"SELECT {base_1},{base_2} FROM product inner join viewed_before on viewed_before.productid = product.id")#where(viewed_before.profileprofile_id = '{profile_id}')
    records = cur.fetchall()
    #print(records)
    sql= []
    done_records=[]
    i = 0
    for record in records:
        #print(record)
        if f'{record[0]}_{record[1]}' in done_records:
            pass
        else:
            done_records.append(f'{record[0]}_{record[1]}')
            cur.execute(f"select profileprofile_id from viewed_before inner join product on viewed_before.productid = product.id where (product.{base_1} ='{record[0]}' and product.{base_2} ='{record[1]}' ) ")
            profID = cur.fetchall()
            combinations = {}
            if len(profID) != 0:
                for id in profID:
                    if id[0] not in combinations:
                        combinations[id[0]] = 1
                    else:
                        combinations[id[0]] +=1
                highest = []
                for x in range(0,5):
                    highest.append(max(combinations, key=combinations.get))
                    del combinations[highest[x]]
                sql.append((record[0],record[1],str(highest)))
                #
                i+=1
                # if i % 100 == 0:
                print(i,datetime.datetime.now() - time0)
    print(i, datetime.datetime.now() - time0,"executmany...")
    cur.executemany("INSERT INTO collaborative_recommendations VALUES (%s,%s,%s);", sql)
    con.commit()
    print(datetime.datetime.now() - time0, 'after executemany')

test('sub_category','gender')
cur.close()
con.close()
