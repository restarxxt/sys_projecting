from threading import Thread
from threading import Lock
import time
import psycopg2

conn = psycopg2.connect("host=localhost dbname=lab user=restarxxt password=Passw0rd")
cur = conn.cursor()
cur.execute('update user_counter set counter = 0')
cur.execute('update user_counter set version = 0')

'''
#        CREATING TABLE

cur.execute("CREATE TABLE user_counter (user_id serial PRIMARY KEY, counter integer, version integer);")

# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
cur.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s)", (1, 0, 0))
cur.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s)", (2, 0, 0))
cur.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s)", (3, 0, 0))
cur.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s)", (4, 0, 0))

# Make the changes to the database persistent
conn.commit()
cur.close()
'''

def lost_update():
    cur = conn.cursor()
    for i in range(10000):
        counter = cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", ('1'))
        counter = cur.fetchone()[0]
        counter += 1
        cur.execute("update user_counter set counter = %s where user_id = %s", (counter, 1))
        conn.commit()
    cur.close()


def in_place_update():
    cur = conn.cursor()
    uid = 2
    for i in range(10000):
        query = u"update user_counter set counter = counter + 1 where user_id = %s;"
        cur.execute(query, str(uid))
        conn.commit()
    cur.close()


def row_level_locking():
    for i in range(10000):
        conn = psycopg2.connect("host=localhost dbname=lab user=restarxxt password=Passw0rd")
        cur = conn.cursor()
        counter = cur.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE;", ('3'))
        counter = cur.fetchone()[0]
        counter += 1
        cur.execute(("update user_counter set counter = %s where user_id = %s"), (counter, 3))
        conn.commit()
        cur.close()
        conn.close()


def optimistic_concurrency_locking():
    cur = conn.cursor()
    for i in range(10000):
        while True:
            cur.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", ('4'))
            counter, version = cur.fetchone()
            counter += 1
            cur.execute(("update user_counter set counter = %s, version = %s where user_id = %s and version = %s"), (counter, version + 1, 4, version))
            conn.commit()
            count = cur.rowcount
            if (count > 0):
                break
    cur.close()


start_time = time.time()
threads = [Thread(target=lost_update, args=[]) for _ in range(10)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
print("=======TASK 1=======")
print("--- %s seconds ---" % (time.time() - start_time))
cur.execute('SELECT counter FROM user_counter WHERE user_id = 1')
print('Counter value: %s' % cur.fetchone()[0])


start_time = time.time()
threads = [Thread(target=in_place_update, args=[]) for _ in range(10)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
print("=======TASK 2=======")
print("--- %s seconds ---" % (time.time() - start_time))
cur.execute('SELECT counter FROM user_counter WHERE user_id = 2')
print('Counter value: %s' % cur.fetchone()[0])


start_time = time.time()
cur.close()
conn.close()
threads = [Thread(target=row_level_locking, args=[]) for _ in range(10)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
print("=======TASK 3=======")
print("--- %s seconds ---" % (time.time() - start_time))
conn = psycopg2.connect("host=localhost dbname=lab user=restarxxt password=Passw0rd")
cur = conn.cursor()
cur.execute('SELECT counter FROM user_counter WHERE user_id = 3')
print('Counter value: %s' % cur.fetchone()[0])


start_time = time.time()
threads = [Thread(target=optimistic_concurrency_locking, args=[]) for _ in range(10)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
print("=======TASK 4=======")
print("--- %s seconds ---" % (time.time() - start_time))
cur.execute('SELECT counter FROM user_counter WHERE user_id = 4')
print('Counter value: %s' % cur.fetchone()[0])


print("\n\n\n")
cur.execute('SELECT * FROM user_counter;')
table = cur.fetchall()
for row in table:
    print("USER_ID = ", row[0], )
    print("Counter = ", row[1])
    print("version  = ", row[2], "\n")
cur.close()
conn.close()