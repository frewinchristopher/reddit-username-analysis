import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

try:
    conn = psycopg2.connect("dbname='" + os.environ.get('REDDIT_DATA_DB') + "' user='" + os.environ.get('REDDIT_DATA_DB_USER') + "' host='" + os.environ.get('REDDIT_DATA_DB_HOST') + "' password='" + os.environ.get('REDDIT_DATA_DB_PORT') + "'")
    oDataFrame = pd.read_sql_query('SELECT * FROM user_about', con=conn) # postgresql data to dataframe
except:
    print "Unable to connect to the database"


# currently two columns - username and about_response
oDataFrameJSONColumns = oDataFrame.about_response.apply(pd.Series); # --> parse out json to columns
oDataFrameJSONColumnsData = oDataFrameJSONColumns.data.apply(pd.Series); # --> parse out nested data {} object to columns
oDataFrameJSONColumnsDataNoNaN = oDataFrameJSONColumnsData[pd.notnull(oDataFrameJSONColumnsData['name'])] # remove all entries where username is null
oDataFrameJSONColumnsDataNoNaNUnique = oDataFrameJSONColumnsDataNoNaN.drop_duplicates(subset='name')

# plot 1 - number of user creations on certain days - state actors afoot? (huge spike of user creations on certain days?)
oDataFrameJSONColumnsDataNoNaNUnique['created_utc'] = (pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc'],unit='s')) # convert unix time to human readable
oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date'] = oDataFrameJSONColumnsDataNoNaNUnique['created_utc'].dt.date # remove time part of date time 
oDataFrameJSONColumnsDataNoNaNUnique = oDataFrameJSONColumnsDataNoNaNUnique.sort_values(['created_utc_date']) # sort by date
lCreationDates = pd.value_counts(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date'].values, sort=True) # counts of user creations by date
lCreationDates = lCreationDates.sort_index(ascending=False) # sort by date, not the number of creations
lCreationDates[0:50].plot.barh() # plot it on a horizontal plot so the dates increase in time as you scroll down the plot
plt.show(); # show it!

# let's look at which users were created on those high volume days
oPotentialStateActorUsers = pd.concat([oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-15")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-16")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-19")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-20")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-21")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-22")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-23")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-24")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-25")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-26")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-27")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-28")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-05-31")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-06-01")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-06-02")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-06-03")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2015-06-04")]])

# how many of them have verified email?
sVerifiedEmailCounts = pd.value_counts(oPotentialStateActorUsers['has_verified_email'].values, sort=True) 
sVerifiedCounts = pd.value_counts(oPotentialStateActorUsers['verified'].values, sort=True) 


#



# last few 3 letter names ever created (after attack)
sLastThreeLetterUsers = pd.concat([oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2016-03-16")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2017-08-07")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2017-08-24")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2017-11-14")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2018-02-06")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2018-03-06")],
oDataFrameJSONColumnsDataNoNaNUnique.loc[pd.to_datetime(oDataFrameJSONColumnsDataNoNaNUnique['created_utc_date']) == pd.to_datetime("2018-03-27")]])








# users with most comment_karma
oDataFrameJSONColumnsDataNoNaN = oDataFrameJSONColumnsDataNoNaN.sort_values(['comment_karma'], ascending=False)
oDataFrameJSONColumnsDataNoNaN.iloc[0:50].plot.bar(x='name', y='comment_karma')
plt.show(); # show it!
