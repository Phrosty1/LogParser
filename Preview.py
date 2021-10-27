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
folderEventData = os.path.realpath('../Sandbox_Brian/EventData/') ; folderEventData = os.path.realpath('C:\FolderToPutLogFiles\EsoAnalytics\Sandbox_Brian\EventData')
folderSandBox = os.path.realpath('../Sandbox_Brian/')
def getEventDataframe(event='EVENT_COMBAT_EVENT', days=0):
   if os.path.exists(folderEventData+'/'+event) :
      if days==0: return pd.read_parquet(folderEventData+'/'+event)
      else: return pd.concat(pd.read_parquet(folderEventData+'/'+event+'/'+f) for f in ([f for f in os.listdir(folderEventData+'/'+event) if f.endswith('.parquet')][-days:]))
def resortIndex(eventsDataframe):
   eventsDataframe.sort_values( ['timestamp', 'player', 'seq'], ignore_index=True, inplace=True)
   return eventsDataframe
def cleanName(name): return name.split('^')[0]

EVENT_COMBAT_EVENT = getEventDataframe('EVENT_COMBAT_EVENT')
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
# ALL = resortIndex(getEventDataframe('ALL'))


# display(ALL[ALL['timestamp'].between(1634415375075, 1634415920805)])
# display(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415375075, 1634415920805)])

# bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634418026126, 1634418028020)])
bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415375075, 1634415920805)])
bigbattle = bigbattle[bigbattle['result'].isin(['BEGIN','CRITICAL_DAMAGE','DAMAGE','DOT_TICK','DOT_TICK_CRITICAL'])]
bigbattle.sort_values( ['result', 'abilityId', 'abilityName', 'timestamp', 'seq'], ignore_index=True, inplace=True)
display(bigbattle)
report = bigbattle.groupby(by=['sourceName','abilityName'], observed=True)['hitValue'].describe()
display(report)
report.to_html(folderSandBox+'/'+'output.html')
# display(bigbattle[bigbattle['abilityName']=='Acid Spray'])

# display(getEventDataframe(event='EVENT_COMBAT_EVENT').groupby('player').boxplot())
# df = EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE['inCombat']=='true']
# df = EVENT_PLAYER_COMBAT_STATE; df['Data_lagged'] = df.groupby(['player'])['timestamp'].shift(1)

# df = pd.merge_asof(EVENT_PLAYER_COMBAT_STATE, EVENT_PLAYER_COMBAT_STATE, on='timestamp', by='player', allow_exact_matches=False)
# display(df)

# display(pd.merge_asof(EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="true"], EVENT_PLAYER_COMBAT_STATE[EVENT_PLAYER_COMBAT_STATE.inCombat=="false"],
#          by='player', on='timestamp', direction='forward', # in ["backward", "forward", "nearest"]
#          suffixes=['_left','_right'], 
#          allow_exact_matches=False))

# bigbattle = getEventDataframe('ALL')

bigbattle = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634415375075, 1634415920805)])
# bigbattle = bigbattle[bigbattle['result'].isin(['BEGIN','CRITICAL_DAMAGE','DAMAGE','DOT_TICK','DOT_TICK_CRITICAL'])]
EVENT_UNIT_DEATH_STATE_CHANGED = getEventDataframe('EVENT_UNIT_DEATH_STATE_CHANGED')

# What Ability keeps killing us
# How much is this skill benefiting another?
# ALL = resortIndex(getEventDataframe('ALL'))
# tmp1 = ALL[ALL['timestamp'].between(1634949919302, 1634949919359)]
# display(tmp1)
# tmp2 = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['timestamp'].between(1634949919302, 1634949919359)])
# display(tmp2)
# tmp1.to_html(folderSandBox+'/'+'output.html')

# display(EVENT_UNIT_DEATH_STATE_CHANGED[EVENT_UNIT_DEATH_STATE_CHANGED['isDead']=='false'])
# display(EVENT_UNIT_DEATH_STATE_CHANGED[EVENT_UNIT_DEATH_STATE_CHANGED['unitTag']=='player'])

EVENT_COMBAT_EVENT['sourceName'] = EVENT_COMBAT_EVENT['sourceName'].apply(cleanName)
EVENT_COMBAT_EVENT['targetName'] = EVENT_COMBAT_EVENT['targetName'].apply(cleanName)
combatHits = pd.DataFrame(EVENT_COMBAT_EVENT[EVENT_COMBAT_EVENT['result'].isin(['BEGIN','CRITICAL_DAMAGE','DAMAGE','DOT_TICK','DOT_TICK_CRITICAL'])]).sort_values( ['result', 'abilityId', 'abilityName', 'timestamp', 'seq'], ignore_index=False)
combatHits.dropna(subset=['sourceName'], inplace=True)


display(combatHits['sourceName'])
# combatHits['sourceName'].apply(lambda x : x.split('^')[0])
# combatHits['sourceName'] = combatHits['sourceName'].apply(lambda x : x.split('^')[0])
# display(combatHits)
display(combatHits[combatHits['player'] == combatHits['sourceName']])
# display(cleanName('Hadara Hazelwood^Fx'))
# report = pd.DataFrame(combatHits.groupby(by=['sourceName','abilityName'], observed=True)['hitValue'].describe()) # describe / mean
# report.sort_values( ['hitValue','abilityName'], ignore_index=False, inplace=True)
display(report.sort_values('mean'))
# report.to_html(folderSandBox+'/'+'output.html')

