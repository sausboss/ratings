import blpapi
from optparse import OptionParser
import pandas as pd
import numpy as np
import pytz
import msgpack
import datetime


def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    # parser.add_option("--me",
    #                   dest="maxEvents",
    #                   type="int",
    #                   help="stop after this many events (default: %default)",
    #                   metavar="maxEvents",
    #                   default=1000000)

    (options, args) = parser.parse_args()

    return options


def createSession():
    """
    Creates Session
    --------------------------
    ARGS:
    RETURNS: Session object
    """
    options = parseCmdLine()
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print "Failed to start session."
        return
    return session


def getSingleField(security, field):
    """
    Returns DataFrame with securities and fields
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', security)
    request.append("fields", field)

    session.sendRequest(request)

    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        if fieldData.numElements() == 0: raise ValueError('N/A Value')
                        for field in fieldData.elements():
                            return field.getValue()
    finally:
        endSession(session)


def updateHistoricalTickData(security, max_days_back=120, minute_interval=1):
    """
    Update historical tick data through today
    """
    def processEventData(eventData):
        """
        Process Data from individual event
        """
        # For stocks
        if security.split(' ')[-1] == 'Equity':
            tickDataArray = msg.getElement(blpapi.Name("tickData"))
            tickDataArray = tickDataArray.getElement(1)

            # Create list that will hold dictionarys representing rows
            rows_list = []

            # Loop through all trades
            for eventData in tickDataArray.values():

                # Get time, convert to EST
                time = eventData.getElement(0).getValue(0)
                time = pytz.timezone('UTC').localize(time).astimezone(pytz.timezone('America/New_York'))
                price = eventData.getElement(2).getValue(0)
                size = eventData.getElement(3).getValue(0)
                try: codes = eventData.getElement(4).getValue(0)
                except: codes = ''
                row = {
                    'Price': price,
                    'Size': size,
                    'Codes': codes,
                    'Time': time
                }
                rows_list.append(row)

        # For FX
        if security.split(' ')[-1] == 'Curncy':
            barDataArray = msg.getElement(blpapi.Name("barData"))
            barDataArray = barDataArray.getElement(1)

            # Create list that will hold dictionarys representing rows
            rows_list = []

            # Loop through all trades
            for eventData in barDataArray.values():

                # Get time, convert to UTC
                time = eventData.getElement(0).getValue(0)
                time = pytz.timezone('UTC').localize(time).astimezone(pytz.timezone('America/New_York'))
                price = eventData.getElement(4).getValue(0)
                row = {
                    'Price': price,
                    'Time': time
                }
                rows_list.append(row)

        return rows_list

    # Start session and create request
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")

    # Set HDF5 info
    DBfile = 'E:/TickData/' + security.replace(' ', '_').replace('/', '-') + '.h5'
    store = pd.HDFStore(DBfile)
    dataset_path = 'ticks'

    # Get todays datetime at 00:00 (UTC)
    today = datetime.datetime.today()
    today = datetime.datetime(day=today.day, month=today.month, year=today.year)
    today = pytz.timezone('America/New_York').localize(today).astimezone(pytz.timezone('UTC'))

    # If stock, get tick data
    if security.split(' ')[-1] == 'Equity':
        # Set request info
        request = refDataService.createRequest("IntradayTickRequest")
        request.set('security', security)
        request.append("eventTypes", 'TRADE')
        request.append("eventTypes", 'AT_TRADE')
        request.set("endDateTime", today - datetime.timedelta(seconds=1))
        request.set("includeNonPlottableEvents", "True")
        request.set("includeConditionCodes", "True")

    # If currency, get bars
    if security.split(' ')[-1] == 'Curncy':
        request = refDataService.createRequest("IntradayBarRequest")
        request.set('security', security)
        request.set("eventType", 'TRADE')
        request.set("interval", minute_interval)
        request.set("endDateTime", today)

    # Check if dataframe already exists, get last index
    try:
        nrows = store.get_storer(dataset_path).nrows
        last_index = store.select('ticks', start=nrows-1, stop=nrows).index[0]

        # Set start to next day at 00:00 (UTC)
        next_day = last_index + datetime.timedelta(days=1)
        start = datetime.datetime(day=next_day.day, month=next_day.month, year=next_day.year)
        start = pytz.timezone('America/New_York').localize(start).astimezone(pytz.timezone('UTC'))

        # If no new days passed since last update, exit
        if today == start:
            print security + ' ---> UP TO DATE'
            store.close()
            return

        # Set start time
        request.set("startDateTime", start)

    # If No tick data yet
    except:
        # Set start to max_days before end
        start = today - datetime.timedelta(days=max_days_back)
        request.set("startDateTime", start)

    # Send request
    session.sendRequest(request)

    print '---- Started Processing ' + security + '----'

    loop = True
    # Start message loop
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:

                    # Process msg
                    rows_list = processEventData(msg)

                    # Create DataFrame from output
                    try:
                        # Create dataframe for message
                        output = pd.DataFrame(rows_list)
                        output.index = output.Time
                        output = output.drop('Time', 1)
                    # If AttributeError, means no new data
                    except AttributeError:
                        print security + ' ---> NO NEW DATA'
                        return

                    # Append message data to database
                    store.append(dataset_path, output, min_itemsize=200, format='table', data_columns=True)
                    print security + ' write, last ' + str(output.index.values[-1])

                    # If final event, end loop
                    if event.eventType() == blpapi.Event.RESPONSE:
                        print '---- Finished Processing ' + security + '----'
                        loop = False
    finally:
        endSession(session)
    # Close DB access
    store.close()


def getHistoricalFields(securities, fields, startDate, endDate, periodicity='DAILY'):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")

    if type(securities) == str:
        securities = [securities]
    if type(fields) == str:
        fields = [fields]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)
    if type(fields) is not pd.core.series.Series:
        fields = pd.Series(fields)

    for security in securities:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)
    request.set("startDate", startDate)
    request.set("endDate", endDate)
    request.set("periodicitySelection", periodicity)

    session.sendRequest(request)

    output = pd.DataFrame(index=securities.values, columns=fields)
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityData = msg.getElement(blpapi.Name("securityData"))
                    security = securityData.getElementAsString(blpapi.Name("security"))
                    fieldData = securityData.getElement(blpapi.Name("fieldData"))
                    for field in range(fieldData.numValues()):
                        fields = fieldData.getValue(field)
                        for n in range(1, fields.numElements()):
                            name = str(fields.getElement(n).name())
                            output.loc[security][name] = fields.getElement(n).getValue()
                    filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getFields(securities, fields):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    if type(fields) == str:
        fields = [fields]
    if type(securities) == str:
        securities = [securities]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)
    if type(fields) is not pd.core.series.Series:
        fields = pd.Series(fields)

    for security in securities:
        request.append("securities", security)
    for field in fields:
        request.append("fields", field)

    session.sendRequest(request)

    output = pd.DataFrame(index=securities.values, columns=fields)
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            output.loc[security][str(field.name())] = field.getValue(0)
                        filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getFieldsOverride(securities, field, override_fields, override_values):
    """
    Returns DataFrame with securities and fields
    """

    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    if type(securities) == str:
        securities = [securities]

    if type(securities) is not pd.core.series.Series:
        securities = pd.Series(securities)

    for security in securities:
        request.append("securities", security)
    request.append("fields", field)

    overrides = request.getElement("overrides")

    if type(override_fields) == str:
        override_fields = [override_fields]

    if type(override_values) == str:
        override_values = [override_values]

    for fld, val in zip(override_fields, override_values):
        override = overrides.appendElement()
        override.setElement("fieldId", fld)
        override.setElement("value", val)

    session.sendRequest(request)
    output = pd.DataFrame(index=securities.values, columns=[field])
    filled = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        security = securityData.getElementAsString(blpapi.Name("security"))
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            try:
                                fieldName = str(field.getValue(0).name())
                            except:
                                fieldName = ""
                            if fieldName == "ADVERTISED_TOTAL_VOLUME_RANK":
                                output.loc[security][fieldName] = field.getValue(0).getElement(2).getValue()
                            else:
                                output.loc[security][str(field.name())] = field.getValue(0)
                        filled.append(security)
            loop = not(len(filled) == len(output))
    finally:
        endSession(session)
    return output


def getBulkAnalystRecs(stock):
    """
    Returns DataFrame of historical analyst ratings for Equity security
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', stock + ' Equity')
    request.append('fields', 'BEST_ANALYST_RECS_BULK')

    session.sendRequest(request)
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            ratings = field.values()
                            output = pd.DataFrame(index=np.arange(field.numValues()), columns=['Firm Name', 'Analyst', 'Recommendation', 'Rating', 'Action Code', 'Target Price', 'Period', 'Date', 'BARR', '1 Year Return'])
                            for i, rating in enumerate(ratings):
                                for element in rating.elements():
                                    fld = element.name().__str__()
                                    output.loc[i][fld] = element.getValue()
                        return output
    finally:
        endSession(session)


def getIndexMembers(index):
    """
    Returns Equity index members as Pandas Series, opens and closes session
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append("securities", index)
    request.append("fields", "INDX_MEMBERS")

    session.sendRequest(request)

    output = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            for val in field.values():
                                for symb in val.elements():
                                    output.append(symb.getValue() + " Equity")
                    loop = False
    finally:
        endSession(session)

    return pd.Series(output, name=index)


def endSession(session):
    """
    Ends Current Session
    --------------------------
    ARGS:
    session - Session object
    RETURNS:
    """
    session.stop()


def startSubscriptions(session, security, field, interval=0):
    """
    Start a subscription
    --------------------------
    ARGS:
    session - Session object
    security - Security
    field - Field
    """
    subscriptions = blpapi.SubscriptionList()
    subscriptions.add(security, field, "interval=%s"%(interval), blpapi.CorrelationId(security))
    session.subscribe(subscriptions)


def formatDate(date):
    """
    Takes datetime.date or datetime.datetime object and formats BBG date string
    """
    return date.strftime('%Y%m%d')


def RTmessageToPandas(message):
    """
    Convert a subscription blpapi Message Object to Pandas Series
    --------------------------
    ARGS:
    message - blpapi Message Object
    RETURNS:
    Pandas DataFrame object
    """

    output = {}

    # Get number of elements in Message Object
    num_elements = message.numElements()
    # Iterate through Elements
    for i in range(num_elements):
        # Get Element Name and convert to string
        name = str(message.getElement(i).name())
        # If Element is in Null State (no Value attached) post "N/A"
        if message.getElement(i).isNull() is True:
            value = "N/A"
        else:
            # If message is Name object convert to string
            try:
                if type(message.getElement(i).getValue()) == blpapi.name.Name:
                    value = str(message.getElement(i).getValue())
                else:
                    value = message.getElement(i).getValue()
            except ValueError:
                print "ValueError"
        output[name] = value

    return pd.Series(output)


def getNonSettlementDates(CDRCode):
    """
    Returns Series of dates given 'CDR_COUNTRY_CODE'
    """
    session = createSession()
    if not session.openService("//blp/refdata"):
        print "Failed to open //blp/refdata"
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")

    request.append('securities', 'USD Curncy')
    request.append('fields', 'CALENDAR_NON_SETTLEMENT_DATES')

    overrides = request.getElement("overrides")
    override_fields = ['SETTLEMENT_CALENDAR_CODE']
    override_values = [CDRCode]

    for fld, val in zip(override_fields, override_values):
        override = overrides.appendElement()
        override.setElement("fieldId", fld)
        override.setElement("value", val)

    session.sendRequest(request)
    output = []
    loop = True
    try:
        while(loop):
            event = session.nextEvent()
            for msg in event:
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    securityDataArray = msg.getElement(blpapi.Name("securityData"))
                    for securityData in securityDataArray.values():
                        fieldData = securityData.getElement(blpapi.Name("fieldData"))
                        for field in fieldData.elements():
                            ratings = field.values()
                            for i, rating in enumerate(ratings):
                                for element in rating.elements():
                                    output.append(element.getValue())
                        return output
    finally:
        endSession(session)


def isExchangeHolidayByTicker(ticker, date):
    """
    Takes list-like tickers and returns dictionary with
    Series exchange holidays for each ticker
    """

    result = False

    # fing country code
    country = ticker.split()[1]

    # screen for several null exchange fields
    if country == 'US':
        code = '#A'
    elif country == 'LN':
        code = 'LS'
    elif country == 'SW':
        code = '#V'
    else:
        code = getFields(ticker, "CDR_EXCH_CODE").values[0][0]

    dates = getNonSettlementDates(code)

    for i in dates:
        if i == date:
            result = True
            break

    return result


def getExchangeTimesByTicker(ticker):
    """
    Bloomberg trade codes to ignore during VWAP calculations. Ticker like "MSFT US Equity"
    """
    # Read file
    with open('exchangeTimes', 'r') as myfile:
        return msgpack.unpackb(myfile.read())[ticker.split(' ')[1]]


def nearEarnings(ticker, today, nextSession, prevSession):
    """
    takes 3 datetime objects and returns bolean indicating if an earnings event alines with either date
    """

    # add 'US' to American tickers, and properly format for Bloomberg API
    company = ticker.split()[0]

    try:
        country = ticker.split()[1]
    except:
        country = 'US'

    ticker = company + ' ' + country + ' Equity'

    earnings = False

    # find last earnings report date
    try:
        announcement = getSingleField(ticker, 'Latest_Announcement_DT')

        if nextSession == announcement or prevSession == announcement or today == announcement:
            earnings = True
    except:
        pass

    # find next earnings report date
    try:
        exAnn = getSingleField(ticker, 'Expected_Report_DT')

        if today == exAnn or nextSession == exAnn or prevSession == exAnn:
            earnings = True
    except:
        pass

    return earnings
