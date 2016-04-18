import psycopg2
import pandas as pd
import userInfo
import pdb


"""
Contains functions to retrive information from DB via commonly used queries.
Funtcions that return dataframes, to be used in ipython for further analysis.
"""


# get login information
loginValues = userInfo.loginValues

def connect():
    """
    Establish connection to DB
    """

    return psycopg2.connect(loginValues['DB'])


def firmHistory(ticker, firm):
    """
    takes in strings, checks perfomance of past events by firms by tickers, returns dataframe
    """

    # use like to grab all instances of a firm
    firm = '%' + firm + '%'

    # SQL request
    DB = connect()
    df = pd.read_sql("select t1date, analyst, type, rating, performancepct from ratings_change join performance on ratings_change.id = performance.event_id where ticker = %(ticker)s and firm like %(firm)s and earnings = False", DB, params={'ticker' : ticker, 'firm' : firm})

    DB.close()

    return df

def tickerMoving(ticker):
    """
    takes in ticker string, and returns the 6 events that moved the stock the most in either direction
    """

    # establish DB connection
    DB = connect()

    upDf = pd.read_sql("select analyst, firm, type, rating, performancepct from ratings_change join performance on ratings_change.id = performance.event_id where ticker = %(ticker)s and earnings = False order by performancepct DESC limit 6", DB, params={'ticker' : ticker})

    downDf = pd.read_sql("select analyst, firm, type, rating, performancepct from ratings_change join performance on ratings_change.id = performance.event_id where ticker = %(ticker)s and earnings = False order by performancepct ASC limit  6", DB, params={'ticker' : ticker})

    DB.close()

    print upDf
    print ""
    print downDf


def analystReturn(name):
    """
    returns median firm performance on action calls
    """

    DB = connect()
    upgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where analyst = %(analyst)s and type = 'upgrade' and earnings = False", DB, params={'analyst' : name})

    downgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where analyst = %(analyst)s and type = 'downgrade' and earnings = False", DB, params={'analyst' : name})

    DB.close()

    # gets median absolute performance of all "action" calls
    (performance, count) = absolutePerformance(upgradeDf, downgradeDf)

    print name
    print "Action Calls"
    print " | " + str(performance) + " | "  "# of calls = " + str(count)


def firmReturn(firmName):
    """
    returns median firm performance on action calls
    """

    firm = '%' + firmName + '%'

    DB = connect()
    upgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where firm like %(firm)s and type = 'upgrade' and earnings = False", DB, params={'firm' : firm})

    downgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where firm like %(firm)s and type = 'downgrade' and earnings = False", DB, params={'firm' : firm})

    # finds all names used in DB for the firm

    dbNames = pd.read_sql("select firm from ratings_change where firm like %(firm)s group by firm", DB, params={'firm': firm})

    DB.close()

    # gets median absolute performance of all "action" calls
    (performance, count) = absolutePerformance(upgradeDf, downgradeDf)

    print dbNames.values
    print "Action Calls"
    print " | " + str(performance) + " | "  "# of calls = " + str(count)


def firmPerformanceByRegion(firmName, fx):
    """
    Looks at how effective each firm is by region
    """

    # make sure FX is uppercase
    fx = fx.upper()

    # allow search to find frim without exact DB name
    firm = '%' + firmName + '%'

    DB = connect()
    upgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where firm like %(firm)s and type = 'upgrade' and earnings = False and fx = %(fx)s", DB, params={'firm' : firm, 'fx' : fx})

    downgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where firm like %(firm)s and type = 'downgrade' and earnings = False and fx = %(fx)s", DB, params={'firm' : firm, 'fx' : fx})

    DB.close()

    # gets median absolute performance of all "action" calls
    (performance, count) = absolutePerformance(upgradeDf, downgradeDf)

    print firmName
    print "Action Calls"
    print " | " + str(performance) + " | "  "# of calls = " + str(count)


def absolutePerformance(upgradeDf, downgradeDf):

    count = 0
    upPfList = []
    downPfList = []

    upRatingList = ['outperform', 'buy', 'overweight', 'accumulate', 'sector outperformer', 'market outperform', 'add']
    downRatingList = ['underperform', 'sell', 'underweight', 'accumlate', 'sector underperformer', 'market underperform', 'reduce']

    # sort events to consider only "action" rating performance
    for event in upgradeDf['rating']:
        if event in upRatingList:
            buyRated = True
        else:
            buyRated = False

        upPfList.append(buyRated)

    # add list as new column
    upgradeDf['buyRated'] = upPfList

    # scrub df for non-action calls
    upgradeDf = upgradeDf[upgradeDf.buyRated == True]

    # total amount of up cases
    upCount = upgradeDf.count().values[0]

    # calculate median pt from listy
    medianUp = upgradeDf['performancepct'].median()

    # repeat for downgrades
    for event in downgradeDf['rating']:
        if event in downRatingList:
            sellRated = True
        else:
            sellRated = False

        downPfList.append(sellRated)

    # add list as new column
    downgradeDf['sellRated'] = downPfList

    # scrub df for non-action calls
    downgradeDf = downgradeDf[downgradeDf.sellRated == True]

    # total amount down cases
    downCount = downgradeDf.count().values[0]
    count = downCount + upCount

    # check for empty dataframe
    if downgradeDf.empty:
        pass
    else:
        # calculate median pt from list
        medianDown = downgradeDf['performancepct'].median()

    # calulate avg analyst return
    try:
        medianSum = medianUp + abs(medianDown)
        avgAbsDayMove = medianSum / 2

    except:
        if medianUp:
            avgAbsDayMove = medianUp
        else:
            avgAbsDayMove = medianDown

    return avgAbsDayMove, count


def pastDate(date):
    """
    takes in date string and returns dataframe with events and coresponding performance 
    """

    DB = connect()
    df = pd.read_sql("select * from ratings_change join performance on ratings_change.id = performance.event_id where t1date = %(date)s and earnings = False", DB, params={'date': date})

    DB.close()

    return df


def fromOpen(date):
    """
    finds day's events where the most return could be gained
    """

    df = pastDate(date)
    outputs = ['ticker', 'sinceOpen', 'type', 'rating', 'firm']

    # caluclate difference between where stock opened and where it closed
    df['sinceOpen'] = df['performancepct'] - df['openpct']

    # sort for differences + or - %2
    df = df[(df.sinceOpen > 2) | (df.sinceOpen < -2)]

    # scrub df of opposite way performance
    upDf = df[(df.sinceOpen > 2) & (df.type == 'upgrade')]
    dnDf = df[(df.sinceOpen < -2) & (df.type == 'downgrade')]

    otherDf = df[(df.type != 'upgrade') & (df.type != 'downgrade')]

    print "Upgrades"
    print "--------"
    print upDf.loc[:, outputs]
    print ""

    print "Downgrades"
    print "----------"
    print dnDf.loc[:, outputs]
    print ""

    print "Other Calls"
    print "-----------"
    print otherDf.loc[:, outputs]
