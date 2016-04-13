Rating Change DB                    version 2.0   04/06/16


GENERAL USAGE NOTES
-------------------


- Parses rating change email maessages, records performance, and performs analysis

- "Action" calls refer to ratings with either a buy or sell connotation

- all userinputs and outputs are meant for ipython use in the terminal



ANALYSIS FUNCTIONS
------------------

sqlQuery:

- firmHistory("ticker", "firm"): checks for events that a broker made on a specific ticker
		arguments: stock ticker, brokerage firm
		** both args are strings

- tickerMoving("ticker"):  finds the 6 events that moved the stock the most in both directions (12 in total).
		arguments: stock ticker
		** string

- analystReturn("Analyst Name"):  takes in string, returns the meadian absolute percent tickers moved on "action" calls, and the count of events, for context.

- frimReturn("Firm Name"):  takes in string, and searchs for firm with names similar.  Returns median absolulte percent tickers moved on action calls, and a list of exact firm names used in DB.

- firmPerformanceByRegion("Firm Name", "fx"):  Looks at how well a brokers' action calls do in a certian region (i.e. Nomura in Europe).

- fromOpen("Date"):  Returns events that have a greater than %2 difference between open and close, respective of call. 

		
---

morningAlert:

set up as a daily check to be run prior to the US open that runs analysis on the days ratings changes.  Functions include printing...

1. street High/Low pt's or upgrades/downgrades in the highest/lowest quartile of the current pt range.

2.  analysts that have moved stocks in the past.  Critera hardcoded...
    	 	  - must have greater than 2 action calls
		  - the absolute day return on action calls must be greater than 2%
3.  anti-consensus calls that are the first in six months (i.e. only downgrade).  NOT NECESSARILY ACTION CALLS.  Requires there be greater than 2 other calls in DB for the ticker.

		  