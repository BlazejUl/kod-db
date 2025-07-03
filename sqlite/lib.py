import sqlite3
import json

def backup():
    conn1 = sqlite3.connect('sqlite/litedata.db')
    backcon = sqlite3.connect('sqlite/backup_litedata.db')
    conn1.backup(backcon)
    conn1.commit()
    backcon.commit()
    backcon.close()
    conn1.close()
    print("Database backup successful")
    return

def restore():
    
    conn1 = sqlite3.connect('sqlite/litedata.db')
    backcon = sqlite3.connect('sqlite/backup_litedata.db')
    backcon.backup(conn1)
    backcon.commit()
    conn1.commit()
    backcon.close()
    conn1.close()
    print("Database restored successfully")
    return

def dropTable(tabname):
    
    conn1 = sqlite3.connect('sqlite/litedata.db')
    c = conn1.cursor()
    try:
        c.execute("""DROP TABLE %s;"""%(tabname))
        print("tabela usunięta")
    except:
        print("błąd podczas usuwania tabeli")
    conn1.commit()
    conn1.close()
    
    return

def jsonToLite(dbPath,jsonPath,tabName,k1,k2,k3,k4):
    
    try:
        cone = sqlite3.connect(dbPath)
        c = cone.cursor()
    except:
        print("błąd połączenia z bazą danych")
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS %s (%s text,%s text,%s text,%s text);""" % (tabName,k1,k2,k3,k4))
        
    except:
        print("Błąd przy tworzeniu/sprawdzeniu czy istnieje")
    cone.commit()
    try:
        f = open(jsonPath)
        tab = json.load(f)
    except:
        print("Błąd przy próbie otworzenia pliku .json")
    finally:
        f.close()

    try:
        for i in tab[tabName]:
            sql = "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s')" % (tabName,i[k1], i[k2], i[k3], i[k4])
            c.execute(sql)
        print("dane poprawnie wprowadzone")
    except:
        print("Błąd podczas wprowadzania danych")

    cone.commit()
    cone.close()
    
    return

def SQLuser_price():
    conn = sqlite3.connect('sqlite/litedata.db')
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
    conn.close()
    return

def SQLdate_price():
    conn = sqlite3.connect('sqlite/litedata.db')
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
    conn.close()
    return

def SQLuser_towar_name():
    conn = sqlite3.connect('sqlite/litedata.db')
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
    conn.close()
    return
    