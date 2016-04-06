import sqlQuery
import pdb, traceback, sys
import pandas as pd
import pytz
import datetime
import BBG


# TODO rewirte code flow using cutoff function

def calculateCutoff():
    """
    returns a datetime object of the last business day, at 15:00 EST
    """

    now = datetime.datetime.now()
    now = pytz.timezone('America/New_York').localize(now).astimezone(pytz.timezone('America/New_York'))
    yesterday = now - pd.tseries.offsets.BDay()
    yesterday = datetime.datetime.date(yesterday)

    cutOff = datetime.datetime.combine(yesterday, datetime.time(15, 00))

    return cutOff


def todaysList():
    """
    returns a pandas data frame of all the email events that happend since 5pm EST yesterday
    """

    cutOff = calculateCutoff()

    DB = sqlQuery.connect()

    df = pd.read_sql("select * from ratings_change where date > %(cutOff)s", DB, params={'cutOff' : cutOff})

    DB.close()

    # calulate surrent median earnings and add to data frame
    df = getMedian(df)

    # adds earnings column to dataframe
    earningsList = []

    # establishes paremters of when to look for earnings events
    now = datetime.datetime.now()
    nextSession = now + pd.tseries.offsets.BDay()
    nextSession = nextSession.strftime('%Y%m%d')
    prevSession = now - pd.tseries.offsets.BDay()
    prevSession = prevSession.strftime('%Y%m%d')

    # takes df, looks at each ticker, compares input dates, returns bool, appends to list
    for ticker in df['ticker']:
        hasEarnings = BBG.nearEarnings(ticker, nextSession, prevSession)
        earningsList.append(hasEarnings)

    # add list as new column
    df['earnings'] = earningsList

    # create df to hold earnings names, for potential future use
    earningsName = df[df.earnings == True]

    # scrub df for events that could be affected by earnings
    df = df[df.earnings == False]

    # print out date and time of report
    now = now.strftime('%m/%d/%Y %H:%M')

    print ""
    print "Report as of"
    print now
    print ""

    return df


def highLowScreen(df):
    """
    screens pandas data frame for highs and lows based on hardcoded assumptions
    """

    # return events with pt's in quartile 4 or 1
    returnFieldsH = ['ticker', 'pt', 'med_pt', 'firm', 'street_high', 'type']
    returnFieldsL = ['ticker', 'pt', 'med_pt', 'firm', 'street_low', 'type']

    # quartile 4, screen for upgrades, if street high, show as well
    print ""
    print "-----------------"
    print "    HIGH PT's    "
    print "-----------------"

    highDf = df[df['pt_quartile'] == 4]
    highDf = highDf[(highDf.type == 'upgrade') | (highDf.street_high == True)]
    highDf = highDf.loc[:, returnFieldsH]

    print highDf

    # reverse for quartile 1
    print ""
    print "-----------------"
    print "    Low PT's     "
    print "-----------------"

    lowDf = df[df['pt_quartile'] == 1]
    lowDf = lowDf[(lowDf.type == 'downgrade') | (lowDf.street_low == True)]
    lowDf = lowDf.loc[:, returnFieldsL]

    print lowDf
    print ""
    print ""
    print ""


def analystScreen(df):
    """
    caluclates analyst past performance, returns name if threshold crossed
    """

    completedAnalysts = []

    # isolate upgrade action calls performance and return median value
    for index, row in df.iterrows():

        name = row['analyst']
        firm = row['firm']

        # check to see if analyst already done
        if name not in completedAnalysts:

            avgAbsDayMove = 0
            count = 0

            # retrieve past event performance, based on analyst
            DB = sqlQuery.connect()
            upgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where analyst = %(analyst)s and type = 'upgrade' and earnings = False", DB, params={'analyst' : name})

            downgradeDf = pd.read_sql("select performancepct, rating from performance join ratings_change on performance.event_id = ratings_change.id where analyst = %(analyst)s and type = 'downgrade' and earnings = False", DB, params={'analyst' : name})

            DB.close()

            # list of "action" ratings
            upRatingList = ['outperform', 'buy', 'overweight', 'accumulate', 'sector outperformer', 'add']
            downRatingList = ['underperform', 'sell', 'underweight', 'accumlate', 'sector underperformer', 'reduce']

            upPfList = []
            downPfList = []

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

            # filter for > 2% impact on stocks rated
            if avgAbsDayMove > 2 and count > 2:

                print name + " | " + firm + " | " + str(avgAbsDayMove) + "  #events = " + str(count)

                analystDf = df[df.analyst == name]
                analystDf = analystDf.loc[:, ['ticker', 'pt', 'med_pt', 'type', 'rating']]

                # print all events publised today by analyst
                print analystDf
                print ""
                print ""
                print ""

            completedAnalysts.append(name)


def antiConsensus(df):
    """
    indicates "action" call is opposite the Street in the specified time frame
    """

    df = df[df.type.isin(['upgrade', 'downgrade'])]


    DB = sqlQuery.connect()
    eventDf = pd.read_sql("select * from ratings_change", DB)

    DB.close()

    # time-frame is set for last 6 months, returns datetime object
    timeFrame = datetime.datetime.now() - datetime.timedelta(days=6*365/12)
    timeFrame = pytz.timezone('America/New_York').localize(timeFrame)

    eventDf = eventDf[eventDf.date > timeFrame]

    print ""
    print "ANTI CONSENSUS CALLS"
    print "--------------------"

    for index, row in df.iterrows():

        idNumber = row['id']
        ticker = row['ticker']
        t = row['type']

        # find past events for respective ticker
        pastList = eventDf[eventDf.ticker == ticker]

        # account for event already existing in DB
        pastList = pastList[pastList.id != idNumber]

        count = pastList.count().values[0]

        # specifies number of events to screen by
        if count > 2:

            typeList = pastList[pastList.type == t]

            if typeList.empty:

                print ""
                print "First " + row['type'] + " in 6 months"
                print ticker + " | " + row['analyst'] + " | " + row['firm'] + " | " + row['rating'] + " | " + "  #events = " + str(count)

            else:
                pass

        else:
            pass


def getMedian(df):
    """
    takes in pandas DF, returns df with added Median PT column
    """

    ptList = []

    for ticker in df['ticker']:

        DB = sqlQuery.connect()
        tickerPts = pd.read_sql("select pt from updated_ratings where ticker = %(ticker)s and pt is not null", DB, params={'ticker' : ticker})

        # calculate median pt from list
        medPT = tickerPts['pt'].median()

        # add to list
        ptList.append(medPT)

    # adds list as new column
    df['med_pt'] = ptList

    return df


if __name__ == '__main__':
    try:
        df = todaysList()
        highLowScreen(df)
        analystScreen(df)
        antiConsensus(df)

    except:
        type, value, tb, = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
