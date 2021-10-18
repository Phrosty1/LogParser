import re, pandas as pd, numpy as np, os, json
from io import StringIO
from IPython.core.display import display
import time # time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1634309423832/1000))
eventSpecsFolder = os.path.realpath('../ESOUIDocumentation/')
rawLogFolder = os.path.realpath('../AnyLoggerRawLuaDumps/')
# eventDataFolder = 'C:/Beachhead/Misc/LogTinker/outdata/'
eventDataFolder = os.path.realpath('../Sandbox_Brian/EventData')
eventDataFolder = os.path.realpath('C:\FolderToPutLogFiles\EsoAnalytics\Sandbox_Brian\EventData')

csvdtype = {'seq': int, 'timestamp': 'int64', 'player': 'category', 'event': 'category'}
csvNumArgs = ["arg"+str(i) for i in range(1, 25)]
for arg in csvNumArgs: csvdtype[arg] = 'str'
csvcols = ";".join(csvdtype.keys())
rawFmtCSV = re.compile("\[(\d+)\]\s?=\s?\"(.*?)\",\s*")

def switchType(intype):
    if intype == 'boolean': return 'category'
    elif intype == 'string': return 'str'
    else: return intype
def loadRaw(sourcefilename):
    with open(sourcefilename, "r", encoding='utf8') as sourcefile:
        pseudocsv = sourcefile.read()
        pseudocsv = pseudocsv[pseudocsv.find("[1]"):pseudocsv.rfind("\",")+2]
        pseudocsv = pseudocsv.replace("\\\"",":quot:")
        pseudocsv = rawFmtCSV.sub("\\1;\\2\\n", pseudocsv) # pseudocsv = re.sub("\[(\d+)\]\s?=\s?\"(.*?)\",\s*", "\\1;\\2\\n", pseudocsv)
        pseudocsv = pseudocsv.replace(":quot:","\\\"")
        return pd.read_csv(StringIO(csvcols+"\n"+pseudocsv), dtype=csvdtype, sep=";", header=0)
def mergeDataFrameIntoFile(eventDataFrame, fileNameEventDate): # pd.read_parquet(fileNameEventDate).append(eventDataFrame).drop_duplicates(subset=["seq", "timestamp", "player"]).to_parquet(fileNameEventDate)
   try:
      fileDataFrame = pd.read_parquet(fileNameEventDate)
   except FileNotFoundError:
      eventDataFrame.to_parquet(fileNameEventDate) # create new file since none exists
   else:
      cnt = len(fileDataFrame)
      fileDataFrame = fileDataFrame.append(eventDataFrame)
      fileDataFrame.drop_duplicates(subset=["player", "timestamp", "seq"], inplace=True)
      if len(fileDataFrame) != cnt :
         fileDataFrame.to_parquet(fileNameEventDate) # appending records made a difference so update the file

# jsonArgnameByArgnumByEvent = json.load(open(eventSpecsFolder+'/'+'ArgnameByArgnumByEvent.json'))
# jsonDtypeByArgnameByEvent = json.load(open(eventSpecsFolder+'/'+'DtypeByArgnameByEvent.json'))
jsonFinalEventArgsInfo = json.load(open(eventSpecsFolder+'/'+'FinalEventArgsInfo.json'))
events = []
allEventArgs = {}
if not os.path.exists(eventDataFolder+'/'+'ALL'): os.makedirs(eventDataFolder+'/'+'ALL')
for eventName, eventArgDetails in jsonFinalEventArgsInfo.items():
   # if eventName == "EVENT_ABILITY_PROGRESSION_XP_UPDATE":
      destEventPath = eventDataFolder+'/'+eventName
      if not os.path.exists(destEventPath): os.makedirs(destEventPath)
      eventDfCols = ['seq', 'timestamp', 'player']
      eventDfCols.extend(eventArgDetails.keys())
      eventArgRename = {argNum:argDetail['name'] for argNum, argDetail in eventArgDetails.items()}
      eventArgTypes = {argDetail['name']:switchType(argDetail['pandastype']) for argDetail in eventArgDetails.values()}
      events.append({'eventName':eventName, 'destEventPath':destEventPath, 'eventDfCols':eventDfCols, 'eventArgRename':eventArgRename, 'eventArgTypes':eventArgTypes})
      allEventArgs[eventName] = {'eventName':eventName, 'destEventPath':destEventPath, 'eventDfCols':eventDfCols, 'eventArgRename':eventArgRename, 'eventArgTypes':eventArgTypes}
jsonArgnameByArgnumByEvent = None
jsonDtypeByArgnameByEvent = None

rawfiles = [f for f in os.listdir(rawLogFolder) if f.endswith('.lua')]
rawfiles = rawfiles[:2]
# rawfiles = [f for f in rawfiles if f=="2021-10-16_20-19-06_SAMANTHA-GAMING.lua"]
for rawfile in rawfiles:
   begintime = int(round(time.time() * 1000))
   df = loadRaw(rawLogFolder+'/'+rawfile)
   fileprefix = time.strftime('%Y-%m-%d', time.localtime(df['timestamp'].values[-1]/1000))
   fileNameEventDate = eventDataFolder+'/'+'ALL'+'/'+fileprefix+'.parquet'
   # df = df[df['event'] == 'EVENT_ABILITY_LIST_CHANGED']
   # display(df[df['event'].isin(allEventArgs.keys())])
   # EVENT_ABILITY_LIST_CHANGED
   # dfAllGrp = df[df['event'].isin(allEventArgs)] # .groupby('event').filter(lambda x: x.name == 'EVENT_ACHIEVEMENT_UPDATED')
   # for eventName, grpdf in dfAllGrp:
   # for grpdf in df[df['event'].isin(allEventArgs)].groupby('event'):
   #    if eventName in allEventArgs:
   for eventName, grpdf in df.groupby('event'):
      if eventName in allEventArgs:
         e = allEventArgs[eventName]
         eventDataFrame = pd.DataFrame(grpdf, columns=e['eventDfCols']).rename(columns=e['eventArgRename']).astype(dtype=e['eventArgTypes'])
         mergeDataFrameIntoFile(eventDataFrame, fileNameEventDate)
   print(rawfile, (int(round(time.time() * 1000))-begintime))
# begintime = int(round(time.time() * 1000))
# for rawfile in rawfiles:
#    df = loadRaw(rawLogFolder+'/'+rawfile)
#    fileprefix = time.strftime('%Y-%m-%d', time.localtime(df['timestamp'].values[-1]/1000))
#    fileNameEventDate = eventDataFolder+'/'+'ALL'+'/'+fileprefix+'.parquet'
#    print(fileNameEventDate, (int(round(time.time() * 1000))-begintime))
#    mergeDataFrameIntoFile(df, fileNameEventDate)
#    for e in events:
#       # e['str_eventArgRename'] = str(e['eventArgRename'])
#       # e['str_eventArgTypes'] = str(e['eventArgTypes'])
#       eventDataFrame = pd.DataFrame(df[df["event"] == e['eventName']], columns=e['eventDfCols']).rename(columns=e['eventArgRename']).astype(dtype=e['eventArgTypes'])
#       if len(eventDataFrame) > 0:
#          fileNameEventDate = e['destEventPath']+'/'+fileprefix+'.parquet'
#          mergeDataFrameIntoFile(eventDataFrame, fileNameEventDate)
#          print(fileNameEventDate, (int(round(time.time() * 1000))-begintime))
