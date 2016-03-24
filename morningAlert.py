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

    # adds earnings column to dataframe
    earningsList = []

    # establishes paremters of when to look for earnings events
    now = datetime.datetime.now()
    nextSession = now + pd.tseries.offsets.BDay()
    nextSession = nextSession.strftime('%Y%m%d')
    prevSession = now - pd.tseries.offsets.BDay()
    prevSession = prevSession.strftime('%Y%m%d')

    # looks up earnings dates by ticker, compares input dates, returns bool, appends to list
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

    return df


def highLowScreen():
    """
    screens pandas data frame for highs and lows based on hardcoded assumptions
    """

    df = todaysList()

    # return events with pt's in quartile 4 or 1
    returnFieldsH = ['ticker', 'pt', 'firm', 'street_high', 'type']
    returnFieldsL = ['ticker', 'pt', 'firm', 'street_low', 'type']

    # quartile 4
    print ""
    print "-----------------"
    print "    HIGH PT's    "
    print "-----------------"

    highDf = df[df['pt_quartile'] == 4]
    highDf = highDf[(highDf.type == 'upgrade') | (highDf.street_high == True)]
    highDf = highDf.loc[:, returnFieldsH]

    print highDf

    # quartile 1
    print ""
    print "-----------------"
    print "    Low PT's     "
    print "-----------------"

    lowDf = df[df['pt_quartile'] == 1]
    lowDf = lowDf[(lowDf.type == 'downgrade') | (lowDf.street_low == True)]
    lowDf = lowDf.loc[:, returnFieldsL]
    pdb.set_trace()

    print lowDf


# def analystScreen(analyst):
#     """
#     alerts if there is an analyst that moves a stock
#     """

#     DB = sqlQuery.connect()

#     df = pd.read_sql("select pctperformance from performance join ratings_change on performancce.event_id = ratings_change.id where date > %(analyst)s", DB, params={'analyst' : analyst}

#     for analyst in list


if __name__ == '__main__':
    try:
        highLowScreen()
    except:
        type, value, tb, = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
