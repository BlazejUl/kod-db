import psycopg
import csv
import simplejson


def connect_db():
    with open("postgresql/database_creds.json") as db_con_file:
        creds = simplejson.loads(db_con_file.read())
        connection = psycopg.connect(
            host=creds['host_name'],
            user=creds['user_name'],
            dbname=creds['db_name'],
            password=creds['password'],
            port=creds['port_number'])
    return connection



def csvToPostgre(conn, path,tabName,k1,k2,k3,k4):
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(10),%s VARCHAR(10),%s VARCHAR(10),%s DATE);""" % (tabName,k1,k2,k3,k4))
        
    except:
        print("Błąd przy tworzeniu/sprawdzeniu czy istnieje")
        
    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        c = conn.cursor()
        int a=0;
        for i in reader:
           if a!=0: 
                try:
                    cursor.execute("INSERT INTO %s VALUES ('%s', '%s', '%s', '%s')" %(tabName,i[k1],i[k2],i[k3],i[k4]))
                    conn.commit()
                except Exception as e:
                    print(f"Błąd przy dodawaniu rekordu: {e}")
           a+=1 
    return



def backup(conn):
    !pg_dump -d student23db > student23db_backup.sql
    return


def restore(conn):
    !pg_restore -d student23db_backup.sql > student23db
    return



def dropTable(conn, table):
    c = conn.cursor()
    c.execute(f'DROP TABLE IF EXISTS {table}')
    conn.commit()
    return


def SQLuser_price(conn):
    c = conn.coursor()
    for wiersz in c.execute("""
        SELECT
        uzytkownicy.username,
        towary.price
        FROM
        tranzakcje
        INNER JOIN
        uzytkownicy ON tranzakcje.user_ID = uzytkownicy.user_ID
        INNER JOIN
        towary ON tranzakcje.item_ID = towary.item_ID"""):
        print(wiersz)
    return

def SQLdate_price(conn):
    c = conn.coursor()
    for wiersz in c.execute("""
        SELECT
        tranzakcje.purchase_date,
        towary.price
        FROM
        tranzakcje
        INNER JOIN
        towary ON tranzakcje.item_ID = towary.item_ID"""):
        print(wiersz)
    return

def SQLuser_towar_name(conn):
    c = conn.coursor()
    for wiersz in c.execute("""
        SELECT
        uzytkownicy.username,
        towary.name
        FROM
        tranzakcje
        INNER JOIN
        uzytkownicy ON tranzakcje.user_ID = uzytkownicy.user_ID
        INNER JOIN
        towary ON tranzakcje.item_ID = towary.item_ID"""):
        print(wiersz)
    return
    
