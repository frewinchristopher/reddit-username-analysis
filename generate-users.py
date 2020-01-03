# imports
import os
import string
import itertools
import psycopg2

lUserNameCharacters = list(string.ascii_lowercase) + list(string.ascii_uppercase) + list(string.digits) + ['_'] # all a-z, A-Z, 0-9 and + _ --> 63 possible lUserNameCharacters
lPossibleUsernames = []
for i in range(1,4):
    lPossibleUsernames = lPossibleUsernames + [''.join(x) for x in itertools.product(lUserNameCharacters, repeat = i)]

# lPossibleUsernamesTuplified = [(x) for x in lPossibleUsernames]
# lPossibleUsernamesTuple = ();
# for i in range(0, len(lPossibleUsernames)):
#     dict = {"username": lPossibleUsernames[i]}
#     lPossibleUsernamesTuple = lPossibleUsernamesTuple + ((dict))
# lPossibleUsernamesTuple = ({"username":x for x in lPossibleUsernames})

# should be total of 16007040 possible usernames... let's write them to the DB!
try:
    conn = psycopg2.connect("dbname='" + os.environ.get('REDDIT_DATA_DB') + "' user='" + os.environ.get('REDDIT_DATA_DB_USER') + "' host='" + os.environ.get('REDDIT_DATA_DB_HOST') + "' password='" + os.environ.get('REDDIT_DATA_DB_PORT') + "'")
except:
    print "Unable to connect to the database"
    
cur = conn.cursor()
# sTemplate = ','.join(['%s'] * len(lPossibleUsernames))
# sQuery = 'INSERT INTO user_about (username) VALUES {}'.format(sTemplate)
# cur.execute(sQuery, lPossibleUsernames)
# sQuery = 'INSERT INTO user_about (username) VALUES %s'
# psycopg2.extras.execute_values (
#     cur, sQuery, lPossibleUsernames, template=None, page_size=100
# )

for i in range(0, len(lPossibleUsernames)):
    cur.execute("INSERT INTO user_about(username) VALUES ('" + lPossibleUsernames[i] + "')")

conn.commit() # commit changes
print "Done."



