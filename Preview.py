import re, pandas as pd, numpy as np, os, json
from io import StringIO
from IPython.core.display import display
import time # time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1634309423832/1000))
# https://mode.com/blog/bridge-the-gap-window-functions/
# https://pandas.pydata.org/pandas-docs/stable/user_guide/cookbook.html#cookbook-merge
# https://stackoverflow.com/questions/44080248/pandas-join-dataframe-with-condition
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html
# https://pandas.pydata.org/docs/reference/api/pandas.merge_asof.html
folderEventSpecs = os.path.realpath('../ESOUIDocumentation/')
folderRawLogs = os.path.realpath('../AnyLoggerRawLuaDumps/')
folderEventData = os.path.realpath('../Sandbox_Brian/EventData/') # ; folderEventData = os.path.realpath('C:\FolderToPutLogFiles\EsoAnalytics\Sandbox_Brian\EventData')
folderSandBox = os.path.realpath('../Sandbox_Brian/')
def getEventDataframe(event='EVENT_COMBAT_EVENT', days=0):
   if os.path.exists(folderEventData+'/'+event) :
      return pd.concat(pd.read_parquet(folderEventData+'/'+event+'/'+f) for f in ([f for f in os.listdir(folderEventData+'/'+event) if f.endswith('.parquet')][-days:]))
def resortIndex(eventsDataframe):
   eventsDataframe.sort_values( ['timestamp', 'player', 'seq'], ignore_index=True, inplace=True)
   return eventsDataframe

EVENT_COMBAT_EVENT = getEventDataframe('EVENT_COMBAT_EVENT', 1)
# display(EVENT_COMBAT_EVENT)
EVENT_PLAYER_COMBAT_STATE = resortIndex(getEventDataframe('EVENT_PLAYER_COMBAT_STATE'))
# display(EVENT_PLAYER_COMBAT_STATE)

CALC_TIMES_IN_COMBAT = pd.DataFrame(pd.merge_asof(EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="true"], EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="false"],
         by='player', direction='forward', # in ["backward", "forward", "nearest"]
         suffixes=['_left','_right'],
         left_index=True, right_index=True, # on='timestamp',
         allow_exact_matches=False)
   , columns=['player','timestamp_left','timestamp_right']).rename(columns={'player':'player', 'timestamp_left':'combat_begin', 'timestamp_right':'combat_end'})
CALC_TIMES_IN_COMBAT['duration'] = CALC_TIMES_IN_COMBAT['combat_end']-CALC_TIMES_IN_COMBAT['combat_begin']
# display(CALC_TIMES_IN_COMBAT.sort_values( ['duration']))

# ALL = resortIndex(getEventDataframe('ALL'))
# display(ALL[ALL['timestamp'].between(1634415375075, 1634415920805)])
# display(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415375075, 1634415920805)])

bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634418026126, 1634418028020)])
bigbattle.sort_values( ['abilityId', 'abilityName', 'timestamp', 'seq'], ignore_index=True, inplace=True)
display(bigbattle)
bigbattle.to_html(folderSandBox+'/'+'output.html')
display(bigbattle.groupby('abilityName').count())
# display(bigbattle[bigbattle['abilityName']=='Acid Spray'])

# display(getEventDataframe(event='EVENT_COMBAT_EVENT').groupby('player').boxplot())
# df = EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE['inCombat']=='true']
# df = EVENT_PLAYER_COMBAT_STATE; df['Data_lagged'] = df.groupby(['player'])['timestamp'].shift(1)

# trades = pd.DataFrame({ 'time': pd.to_datetime(['20160525 13:30:00.023', '20160525 13:30:00.038', '20160525 13:30:00.048', '20160525 13:30:00.048', '20160525 13:30:00.048']), 'ticker': ['MSFT', 'MSFT', 'GOOG', 'GOOG', 'AAPL'], 'price': [51.95, 51.95, 720.77, 720.92, 98.00], 'quantity': [75, 155, 100, 100, 100]}, columns=['time', 'ticker', 'price', 'quantity'])
# quotes = pd.DataFrame({ 'time': pd.to_datetime(['20160525 13:30:00.023', '20160525 13:30:00.023', '20160525 13:30:00.030', '20160525 13:30:00.041', '20160525 13:30:00.048', '20160525 13:30:00.049', '20160525 13:30:00.072', '20160525 13:30:00.075']), 'ticker': ['GOOG', 'MSFT', 'MSFT', 'MSFT', 'GOOG', 'AAPL', 'GOOG', 'MSFT'], 'bid': [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01], 'ask': [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]}, columns=['time', 'ticker', 'bid', 'ask'])
# pd.merge_asof(trades, quotes, on='time', by='ticker')

# df = pd.merge_asof(EVENT_PLAYER_COMBAT_STATE, EVENT_PLAYER_COMBAT_STATE, on='timestamp', by='player', allow_exact_matches=False)
# display(df)

# display(pd.merge_asof(EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="true"], EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="false"],
#          by='player', on='timestamp', direction='forward', # in ["backward", "forward", "nearest"]
#          suffixes=['_left','_right'], 
#          allow_exact_matches=False))

bigbattle = getEventDataframe('ALL')


