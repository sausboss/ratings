import psycopg2
import datetime
import BBG
import pdb, traceback, sys
import pandas as pd
from pandas import Timestamp
import pytz
import userInfo

n = 0
loginValues = userInfo.loginValues

def connect():
    """
    Establish connection to database
    """

    return psycopg2.connect(loginValues['DB'])


def writeToDB(nextSession, prevClose, nextOpen, nextDayHigh, nextDayLow, nextClose, earnings, id):

    performance = ((nextClose - prevClose) / prevClose) * 100
    highpct = ((nextDayHigh - prevClose) / prevClose) * 100
    lowpct = ((nextDayLow - prevClose) / prevClose) * 100
    openpct = ((nextOpen - prevClose) / prevClose) * 100

    DB = connect()
    c = DB.cursor()
    c.execute('Update performance set t1date = (%s), t0close = (%s), open = (%s), high = (%s), low = (%s), t1close = (%s), performancepct = (%s), openpct = (%s), highpct = (%s), lowpct = (%s), earnings = (%s) where event_id = (%s)', (nextSession, prevClose, nextOpen, nextDayHigh, nextDayLow, nextClose, performance, openpct, highpct, lowpct, earnings, id))

    DB.commit()
    DB.close()

    global n
    n += 1

    print ''
    print '--------------------------------'
    print "Preformance added" + " " + str(n) + "  " + str(id)
    print '--------------------------------'
    print ''


def skipEvent(timestamp, id):

    now = datetime.datetime.now()
    now = pytz.timezone('America/New_York').localize(now).astimezone(pytz.timezone('America/New_York'))
    t7 = timestamp + pd.tseries.offsets.Week()

    if now > t7:

        nextSession = Timestamp(timestamp).to_datetime().strftime('%Y%m%d')

        DB = connect()
        c = DB.cursor()
        c.execute('Update performance set t1date = (%s) where event_id = (%s)', (nextSession, id))

        DB.commit()
        DB.close()

        global n
        n += 1

        print ''
        print '--------------------------------'
        print "Event skiped" + " " + str(n) + "  " + str(id)
        print '--------------------------------'
        print ''

    else:
        pass


def getEventIDs():
    """
    finds list of event id's without corresponding price performance data
    """

    idList = []

    # connect to the database
    DB = connect()
    c = DB.cursor()

    # get all US events
    c.execute('select event_id from performance where t1date is NULL')

    idList = c.fetchall()
    DB.close()

    processRow(idList)

def processRow(idList):

    for id in idList:

        id = id[0]
        DB = connect()
        c = DB.cursor()
        c.execute('select date, ticker from ratings_change where id = (%s)', (id,))
        event = c.fetchall()
        DB.close()

        timestamp = event[0][0]
        timestamp = timestamp.replace(tzinfo = None)
        ticker = event[0][1]

        convertData(ticker, timestamp, id)

def convertData(ticker, timestamp, id):
    """
    Takes event information from DB, and returns when the event happens in realation to its trading sessions
    """

    # split out exchange code
    company = ticker.split()[0]
    try:
        country = ticker.split()[1]
    except:
        country = 'US'

    ticker = company + ' ' + country + ' Equity'

    emailDate = datetime.datetime.date(timestamp)
    emailTime = datetime.datetime.time(timestamp)

    try:
        local = BBG.getExchangeTimesByTicker(ticker)
    except:
        local = {'open': [9,30], 'close': [16,00], 'zone': 'America/New_York'}

    cmbClose = datetime.datetime.combine(emailDate, datetime.time(*local['close']))
    estLocalClose = pytz.timezone(local['zone']).localize(cmbClose).astimezone(pytz.timezone('America/New_York'))

    # Takes in email and time/date, returns next session
    
    # account for Isreali workweek
    if country == 'IT':
        pass
    #     (adjLocalClose, nextSession, prevSession) = ilsTradeDates(emailTime, estLocalClose)

    # process as normal work week
    else:
        if emailTime > estLocalClose.time():
            adjLocalClose = estLocalClose + pd.tseries.offsets.BDay()

            # check to see if holiday exists
            if isHoliday(ticker, adjLocalClose):
                while isHoliday(ticker, adjLocalClose):
                    adjLocalClose = adjLocalClose + pd.tseries.offsets.BDay()

            nextSession = Timestamp(adjLocalClose).to_datetime().strftime('%Y%m%d')
            if isHoliday(ticker, estLocalClose):
                while isHoliday(ticker, estLocalClose):
                    estLocalClose = estLocalClose - pd.tseries.offsets.BDay()

            prevSession = Timestamp(estLocalClose).to_datetime().strftime('%Y%m%d')
            
        else:
            adjLocalClose = estLocalClose
            
            # check to see if nextSession is a holiday
            if isHoliday(ticker, adjLocalClose):
                while isHoliday(ticker, estLocalClose):
                    estLocalClose = estLocalClose + pd.tseries.offsets.BDay()
                
            nextSession = Timestamp(estLocalClose).to_datetime().strftime('%Y%m%d')
            # find previous session
            prevSession = estLocalClose - pd.tseries.offsets.BDay()

            # check to see if previous session was holiday
            if isHoliday(ticker, prevSession):
                while isHoliday(ticker, prevSession):
                    prevSession = prevSession - pd.tseries.offsets.BDay()

            prevSession = Timestamp(prevSession).to_datetime().strftime('%Y%m%d')

        # pass on events if the close hasn't yet occuerd
        now = datetime.datetime.now()
        now = pytz.timezone('America/New_York').localize(now).astimezone(pytz.timezone('America/New_York'))

        if now > adjLocalClose:

            findFields(ticker, nextSession, prevSession, id, adjLocalClose)

    
def findFields(ticker, nextSession, prevSession, id, adjLocalClose):
    """
    returns bloomberg fields for given event
    """

    # get price data for session after email
    prevClose = BBG.getHistoricalFields(ticker, 'PX_LAST', prevSession, prevSession)
    nextOpen = BBG.getHistoricalFields(ticker, 'PX_OPEN', nextSession, nextSession)
    nextDayHigh = BBG.getHistoricalFields(ticker, 'PX_HIGH', nextSession, nextSession)
    nextDayLow = BBG.getHistoricalFields(ticker, 'PX_LOW', nextSession, nextSession)
    nextClose = BBG.getHistoricalFields(ticker, 'PX_LAST', nextSession, nextSession)
    
    # check for empty df's and pass up if so
    if prevClose.isnull().values[0][0]:
        skipEvent(adjLocalClose, id)
    elif nextOpen.isnull().values[0][0]:
        skipEvent(adjLocalClose, id)
    elif nextDayHigh.isnull().values[0][0]:
        skipEvent(adjLocalClose, id)
    elif nextDayLow.isnull().values[0][0]:
        skipEvent(adjLocalClose, id)
    elif nextClose.isnull().values[0][0]:
        skipEvent(adjLocalClose, id)
    else:
        prevClose = prevClose.values[0][0]
        nextOpen = nextOpen.values[0][0]
        nextDayHigh = nextDayHigh.values[0][0]
        nextDayLow = nextDayLow.values[0][0]
        nextClose = nextClose.values[0][0]

        earnings = nearEarnings(ticker, nextSession, prevSession)

        writeToDB(nextSession, prevClose, nextOpen, nextDayHigh, nextDayLow, nextClose, earnings, id)

def nearEarnings(ticker, nextSession, prevSession):

    earnings = False
    nextSession = datetime.datetime.strptime(nextSession, '%Y%m%d')
    nextSession = datetime.datetime.date(nextSession)
    
    prevSession = datetime.datetime.strptime(prevSession, '%Y%m%d')
    prevSession = datetime.datetime.date(prevSession)

    # find last earnings report date
    try:
        announcement = BBG.getSingleField(ticker, 'Latest_Announcement_DT')

        if nextSession == announcement or prevSession == announcement:
            earnings = True
    except:
        pass
    
    # find next earnings report date
    try:
        exAnn = BBG.getSingleField(ticker, 'Expected_Report_DT')
        t1 = nextSession + pd.tseries.offsets.BDay()
        t1 = Timestamp(prevSession).to_datetime().strftime('%Y%m%d')
        t1 = datetime.datetime.strptime(t1, '%Y%m%d')
        t1 = datetime.datetime.date(t1)

        if t1 == exAnn or nextSession == exAnn:
            earnings = True
    except:
        pass

    return earnings

# def ilsTradeDates(emailTime, estLocalClose):

#     pdb.set_trace()

#     if emailTime > estLocalClose.time():
#         adjLocalClose = estLocalClose + pd.tseries.offsets.BDay()
#         nextSession = Timestamp(adjLocalClose).to_datetime().strftime('%Y%m%d')
#         prevSession = Timestamp(estLocalClose).to_datetime().strftime('%Y%m%d')
#     else:
#         adjLocalClose = estLocalClose
#         nextSession = Timestamp(estLocalClose).to_datetime().strftime('%Y%m%d')
#         prevSession = estLocalClose - pd.tseries.offsets.BDay()
#         prevSession = Timestamp(prevSession).to_datetime().strftime('%Y%m%d')

#     return (adjLocalClose, nextSession, prevSession)

def isHoliday(ticker, date):
    """
    formats date then returns bool if date is in list of holidays
    """

    date = date.replace(tzinfo = None)
    date = datetime.datetime.date(date)

    result = BBG.isExchangeHolidayByTicker(ticker, date)

    return result

    
# If file is executed as script
if __name__ == '__main__':
    try:
        getEventIDs()
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
