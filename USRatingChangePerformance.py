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
    """
    adds performance values to DB
    """

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
    t7 = timestamp + pd.tseries.offsets.Week()

    # check if event info has been left blank for 7 days
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
    finds list of event id's without corresponding price performance data (t1date)
    """

    idList = []

    # connect to the database
    DB = connect()
    c = DB.cursor()

    # get all events in performance table without a date value
    c.execute('select event_id from performance where t1date is NULL')

    idList = c.fetchall()
    DB.close()

    processRow(idList)
    

def processRow(idList):
    """
    looks up event by id, and returns relevent data for use in determining performance
    """

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
        
        # timestamp is a datetime.datetime object
        convertData(ticker, timestamp, id)

def convertData(ticker, timestamp, id):
    """
    Takes event information from DB, and returns when the event happens in realation to its trading sessions
    """

    # format ticker for Bloomberg API use
    ticker = BBG.bloombergTicker(ticker)

    emailDate = datetime.datetime.date(timestamp)
    emailTime = datetime.datetime.time(timestamp)

    # find local exchange hours
    try:
        local = BBG.getExchangeTimesByTicker(ticker)
    except:
        local = {'open': [9,30], 'close': [16,00], 'zone': 'America/New_York'}

    # converts local close to datetime.datetime object 
    cmbClose = datetime.datetime.combine(emailDate, datetime.time(*local['close']))
    # converts local close to corresponding EST value
    estLocalClose = pytz.timezone(local['zone']).localize(cmbClose).astimezone(pytz.timezone('America/New_York'))

    # find correct dates to document event performance
    # screen for Israeli tickers, and account for irregular business days
    if ticker.split()[1] == 'ILS':

        ILSweek = 'Sun Mon Tue Wed Thu'
        
        if emailTime > estLocalClose.time():
        
            # establish seesion dates, tomorrow is performance date
            nextSession = estLocalClose + pd.tseries.offsets.CDay(weekmask=ILSweek)
            prevSession = estLocalClose

            # check to see if holiday exists next session, if so, adjust date
            if isHoliday(ticker, nextSession):
                while isHoliday(ticker, nextSession):
                    nextSession = nextSession + pd.tseries.offsets.CDay(weekmask=ILSweek)

            # check to see is email date/ previous session was a holiday
            if isHoliday(ticker, prevSession):
                while isHoliday(ticker, prevSession):
                    prevSession = prevSession - pd.tseries.offsets.CDay(weekmask=ILSweek)

        # if email before close, current session as performance date
        else:

            # establish session dates, today is performance date
            nextSession = estLocalClose
            prevSession = estLocalClose - pd.tseries.offsets.CDay(weekmask=ILSweek)

            # check to see if nextSession is a holiday
            if isHoliday(ticker, nextSession):
                while isHoliday(ticker, nextSession):
                    nextSession = nextSession + pd.tseries.offsets.CDay(weekmask=ILSweek)

        # check to see if previous session was holiday
        if isHoliday(ticker, prevSession):
            while isHoliday(ticker, prevSession):
                prevSession = prevSession - pd.tseries.offsets.CDay(weekmask=ILSweek)
                
    # process as normal business week (M-F)
    else:
        if emailTime > estLocalClose.time():
            
            # establish session dates, tomorrow is performance date
            nextSession = estLocalClose + pd.tseries.offsets.BDay()
            prevSession = estLocalClose

            # check to see if holiday exists next session, if so, adjust date
            if isHoliday(ticker, nextSession):
                while isHoliday(ticker, nextSession):
                    nextSession = nextSession + pd.tseries.offsets.BDay()

            # check to see is email date/ previous session was a holiday
            if isHoliday(ticker, prevSession):
                while isHoliday(ticker, prevSession):
                    prevSession = prevSession - pd.tseries.offsets.BDay()

        # if email before close, current session as performance date
        else:
            # establish session dates, today is performance date
            nextSession = estLocalClose
            prevSession = estLocalClose - pd.tseries.offsets.BDay()

            # check to see if nextSession is a holiday
            if isHoliday(ticker, nextSession):
                while isHoliday(ticker, nextSession):
                    nextSession = nextSession + pd.tseries.offsets.BDay()

            # check to see if previous session was holiday
            if isHoliday(ticker, prevSession):
                while isHoliday(ticker, prevSession):
                    prevSession = prevSession - pd.tseries.offsets.BDay()

    # check if nextsession has happend yet, if not, pass
    now = datetime.datetime.now()
    now = pytz.timezone('America/New_York').localize(now).astimezone(pytz.timezone('America/New_York'))

    if now > nextSession:

        # convert timestamp to datetime date
        nextSession = nextSession.date()
        prevSession = prevSession.date()

        # if email before open, or during session, extra day needed to make sure earnings event doesn't interfere with data quality
        if emailTime < estLocalClose.time():
            tomorrow = estLocalClose + pd.tseries.offsets.BDay()
            tomorrow = tomorrow.date()

            earnings = BBG.nearEarnings(ticker, nextSession, tomorrow, prevSession)
        else:
            earnings = findEarnings(ticker, nextSession, prevSession)

        findFields(ticker, nextSession, prevSession, id, earnings)

    else:
        pass

    
def findFields(ticker, nextSession, prevSession, id, earnings):
    """
    returns bloomberg fields for given event
    """

    # save datetime.date object for nextSession, for use in skipEvent()
    tradeDate = nextSession
    
    # convert dates from datetime to strings
    nextSession = datetime.datetime.strftime(nextSession, '%Y%m%d')
    prevSession = datetime.datetime.strftime(prevSession, '%Y%m%d')
    
    # get price data for session after email, and previous session
    prevClose = BBG.getHistoricalFields(ticker, 'PX_LAST', prevSession, prevSession)
    nextOpen = BBG.getHistoricalFields(ticker, 'PX_OPEN', nextSession, nextSession)
    nextDayHigh = BBG.getHistoricalFields(ticker, 'PX_HIGH', nextSession, nextSession)
    nextDayLow = BBG.getHistoricalFields(ticker, 'PX_LOW', nextSession, nextSession)
    nextClose = BBG.getHistoricalFields(ticker, 'PX_LAST', nextSession, nextSession)

    # check for empty df's and pass on event if so
    if prevClose.isnull().values[0][0]:
        skipEvent(tradeDate, id)
    elif nextOpen.isnull().values[0][0]:
        skipEvent(tradeDate, id)
    elif nextDayHigh.isnull().values[0][0]:
        skipEvent(tradeDate, id)
    elif nextDayLow.isnull().values[0][0]:
        skipEvent(tradeDate, id)
    elif nextClose.isnull().values[0][0]:
        skipEvent(tradeDate, id)
    else:
        prevClose = prevClose.values[0][0]
        nextOpen = nextOpen.values[0][0]
        nextDayHigh = nextDayHigh.values[0][0]
        nextDayLow = nextDayLow.values[0][0]
        nextClose = nextClose.values[0][0]

        writeToDB(nextSession, prevClose, nextOpen, nextDayHigh, nextDayLow, nextClose, earnings, id)


def findEarnings(ticker, nextSession, prevSession):
    """
    takes 2 datetime date objects and returns bolean indicating if an earnings event alines with either date
    """

    earnings = False

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
