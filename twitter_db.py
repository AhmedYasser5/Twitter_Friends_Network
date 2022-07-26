import twurl
import urllib.request, urllib.error
import sqlite3
import json

TWITTER_URL = 'https://api.twitter.com/1.1/friends/list.json'
conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('create table if not exists People (id integer primary key, name text unique, retrieved boolean)')
cur.execute('create table if not exists Relations (from_id integer, to_id integer, unique(from_id, to_id))')

while True:
    account = input('Enter account name, \'print\', \'delete\', or \'quit\': ')
    
    if account == 'quit': break

    if account == 'delete':
        account = input('Enter account to delete: ')
        cur.execute('delete from People where name=? returning id', (account, ))
        try:
            ID = cur.fetchone()[0]
            cur.execute('delete from Relations where from_id=? or to_id=?', (ID, ID))
        except:
            print('No account with that name')
        continue

    if account == 'print':
        print('Data stored till now:')
        print('\tPeople Table:')
        cur.execute('select * from People')
        for row in cur:
            print('\t', row, sep='')
        print('\tJoined Tables:')
        cur.execute('select * from People join Relations on People.id=Relations.from_id')
        for row in cur:
            print('\t', row, sep='')
        continue

    if len(account) == 0:
        cur.execute('select id, name from People where retrieved=false limit 1')
        try:
            ID, account = cur.fetchone()
        except:
            print('All people have been retrieved')
            continue
    else:
        try:
            cur.execute('select id, retrieved from People where name=? limit 1', (account, ))
            ID, retrieved = cur.fetchone()
            if retrieved == True:
                print('Account already exists... Enter another one')
                continue
        except:
            cur.execute('insert or fail into People (name, retrieved) values (?, false)', (account, ))
            ID = cur.lastrowid

    url = twurl.augment(TWITTER_URL,
                  {'screen_name': account, 'count': '20'})
    print('Retrieving account', account)

    try:
        connection = urllib.request.urlopen(url)
    except Exception as err:
        print('Failed to Retrieve', err)
        continue

    data = connection.read().decode()
    headers = dict(connection.getheaders())

    print('Remaining', headers['x-rate-limit-remaining'])

    try:
        js = json.loads(data)
    except:
        print('Unable to parse json')
        print(data)
        continue

    if 'users' not in js:
        print('Wrong type of json')
        print(json.dumps(js, indent=4))
        continue

    cur.execute('update People set retrieved=true where id=?', (ID, ))

    oldCount = 0
    newCount = 0
    for user in js['users']:
        name = user['screen_name']
        print('Found', name)
        cur.execute('select id from People where name=?', (name, ))
        
        try:
            friend_id = cur.fetchone()[0]
            oldCount += 1
        except:
            cur.execute('insert or fail into People (name, retrieved) values (?, false)', (name, ))
            friend_id = cur.lastrowid
            newCount += 1

        cur.execute('insert or ignore into Relations (from_id, to_id) values (?, ?)', (ID, friend_id))

    print('New Accounts = ', newCount, ', Revisited = ', oldCount, sep='')
    conn.commit()

cur.close()

