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
      # if days==0: return pd.read_parquet(folderEventData+'/'+event)
      # else: return pd.concat(pd.read_parquet(folderEventData+'/'+event+'/'+f) for f in ([f for f in os.listdir(folderEventData+'/'+event) if f.endswith('.parquet')][-days:]))
      return pd.concat(pd.read_parquet(folderEventData+'/'+event+'/'+f) for f in ([f for f in os.listdir(folderEventData+'/'+event) if f.endswith('.parquet')][-days:]))
def resortIndex(eventsDataframe):
   eventsDataframe.sort_values( ['timestamp', 'player', 'seq'], ignore_index=True, inplace=True)
   return eventsDataframe
def cleanName(name): return name.split('^')[0]

EVENT_COMBAT_EVENT = getEventDataframe('EVENT_COMBAT_EVENT')
# EVENT_COMBAT_EVENT['sourceName'] = EVENT_COMBAT_EVENT['sourceName'].apply(cleanName)
# EVENT_COMBAT_EVENT['targetName'] = EVENT_COMBAT_EVENT['targetName'].apply(cleanName)
EVENT_POWER_UPDATE = resortIndex(getEventDataframe('EVENT_POWER_UPDATE')) # [14909] = "1635293485592;Nissa Forbus;EVENT_POWER_UPDATE;boss1;999;-2;923059;923059;923059",
EVENT_PLAYER_COMBAT_STATE = resortIndex(getEventDataframe('EVENT_PLAYER_COMBAT_STATE'))
CALC_TIMES_IN_COMBAT = pd.DataFrame(pd.merge_asof(EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="true"], EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="false"],
         by='player', direction='forward', # in ["backward", "forward", "nearest"]
         suffixes=['_left','_right'],
         left_index=True, right_index=True, # on='timestamp',
         allow_exact_matches=False)
   , columns=['player','timestamp_left','timestamp_right']).rename(columns={'player':'player', 'timestamp_left':'combat_begin', 'timestamp_right':'combat_end'})
CALC_TIMES_IN_COMBAT['duration'] = CALC_TIMES_IN_COMBAT['combat_end']-CALC_TIMES_IN_COMBAT['combat_begin']
display(CALC_TIMES_IN_COMBAT.sort_values( ['duration']))
ALL = resortIndex(getEventDataframe('ALL'))


display(EVENT_POWER_UPDATE.groupby(['unitTag','powerType'])['powerEffectiveMax'].describe())



# bigbattle = pd.DataFrame(ALL[ALL['timestamp'].between(1634415376230, 1634415920805)], columns=['seq','timestamp','player','event','arg1','arg2','arg3','arg4','arg5','arg6'])
# bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415376230, 1634415920805)])
bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415376230, 1634415920805) & EVENT_COMBAT_EVENT['result'].isin(['BEGIN','CRITICAL_DAMAGE','DAMAGE','DOT_TICK','DOT_TICK_CRITICAL'])]).sort_values( ['result', 'abilityId', 'abilityName', 'timestamp', 'seq'], ignore_index=False)
# bigbattle.dropna(subset=['sourceName'], inplace=True)
bigbattle['sourceName'] = bigbattle['sourceName'].apply(lambda x: str(x))
# bigbattle = bigbattle[bigbattle['sourceName']=='NaN']
display(bigbattle)
report = bigbattle.groupby('sourceName')['hitValue'].describe()
# str(type(name)) # 
display(report)
# report.to_html(folderSandBox+'/'+'output.html')
