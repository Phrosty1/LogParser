import re, pandas as pd, numpy as np, os, json
from io import StringIO
from IPython.core.display import display
import time # time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1634309423832/1000))
folderEventSpecs = os.path.realpath('../ESOUIDocumentation/')
folderRawLogs = os.path.realpath('../AnyLoggerRawLuaDumps/')
# folderEventData = 'C:/Beachhead/Misc/LogTinker/outdata/'
folderEventData = os.path.realpath('../Sandbox_Brian/EventData/')
# folderEventData = os.path.realpath('C:\FolderToPutLogFiles\EsoAnalytics\Sandbox_Brian\EventData')

def switchType(intype):
    if intype == 'boolean': return 'category'
    elif intype == 'string': return 'str'
   #  elif intype == 'category': return 'str' # testing something
    else: return intype
csvdtype = {'seq': int, 'timestamp': 'int64', 'player': 'category', 'event': 'category'}
csvNumArgs = ["arg"+str(i) for i in range(1, 25)]
csvdtype.update({arg:'str' for arg in csvNumArgs})
csvcols = ";".join(csvdtype.keys())
rawFmtCSV = re.compile("\[(\d+)\]\s?=\s?\"(.*?)\",\s*")
def loadRaw(sourcefilename):
   with open(sourcefilename, "r", encoding='utf8') as sourcefile:
      pseudocsv = sourcefile.read()
      pseudocsv = ("\n".join([seqAndData[0]+";"+seqAndData[1] for seqAndData in rawFmtCSV.findall(pseudocsv.replace("\\\"",":quot:"))])).replace(":quot:","\\\"")
      return pd.read_csv(StringIO(csvcols+"\n"+pseudocsv), dtype=csvdtype, sep=";", header=0)
def getAllEventArgs(jsonFinalEventArgsInfo):
   retval = {}
   if not os.path.exists(folderEventData+'/'+'ALL'): os.makedirs(folderEventData+'/'+'ALL')
   for eventName, eventArgDetails in jsonFinalEventArgsInfo.items():
      # if eventName == 'EVENT_COMBAT_EVENT' : # 'EVENT_ABILITY_PROGRESSION_RESULT': # "EVENT_ABILITY_PROGRESSION_XP_UPDATE"
         destEventPath = folderEventData+'/'+eventName
         # if not os.path.exists(destEventPath): os.makedirs(destEventPath)
         eventDfCols = ['seq', 'timestamp', 'player']
         eventDfCols.extend(eventArgDetails.keys())
         eventArgRename = {argNum:argDetail['name'] for argNum, argDetail in eventArgDetails.items()}
         eventArgConstmap = {argDetail['name']:argDetail['constmap'] for argDetail in eventArgDetails.values() if 'constmap' in argDetail}
         eventArgTypes = {argDetail['name']:switchType(argDetail['pandastype']) for argDetail in eventArgDetails.values()}
         retval[eventName] = { 'eventName':eventName, 'destEventPath':destEventPath, 'eventDfCols':eventDfCols, 'eventArgConstmap':eventArgConstmap, 'eventArgRename':eventArgRename, 'eventArgTypes':eventArgTypes}
   return retval
allEventArgs = getAllEventArgs(json.load(open(folderEventSpecs+'/'+'FinalEventArgsInfo.json')))
cntByEventFile = {}
datByEventFile = {}
def mergeDataFrameIntoFilePrep(eventDataFrame, fileNameEventDate): # pd.read_parquet(fileNameEventDate).append(eventDataFrame).drop_duplicates(subset=["seq", "timestamp", "player"]).to_parquet(fileNameEventDate)
   if fileNameEventDate in datByEventFile:
      datByEventFile[fileNameEventDate] = datByEventFile[fileNameEventDate].append(eventDataFrame)
   else:
      try: datByEventFile[fileNameEventDate] = pd.read_parquet(fileNameEventDate); cntByEventFile[fileNameEventDate] = len(datByEventFile[fileNameEventDate])
      except FileNotFoundError: datByEventFile[fileNameEventDate] = eventDataFrame; cntByEventFile[fileNameEventDate] = 0 # set to 0 since nothing was in the file

lstNotInJSON = set()

# Preload ALL files as new (only needed if reloading everything)
for allFile in [f for f in os.listdir(folderEventData+'/'+'ALL') if f.endswith('.parquet')]:
   # if not allFile.startswith('2021-10-16') :
      begintime = int(round(time.time() * 1000))
      eventDataFrame = pd.read_parquet(folderEventData+'/'+'ALL'+'/'+allFile)
      fileprefix = time.strftime('%Y-%m-%d', time.localtime(eventDataFrame['timestamp'].values[-1]/1000))
      fileNameEventDate = folderEventData+'/'+'ALL'+'/'+fileprefix+'.parquet'
      mergeDataFrameIntoFilePrep(eventDataFrame, fileNameEventDate)
      cntByEventFile[fileNameEventDate] = 0
      print("PreLoaded:"+allFile, (int(round(time.time() * 1000))-begintime))

rawfiles = [f for f in os.listdir(folderRawLogs) if f.endswith('.lua') or f.endswith('.lson')]
for rawfile in rawfiles:
   # if rawfile.startswith('2021-10-16_20-19-06_SAMANTHA-GAMING.lua') :
      begintime = int(round(time.time() * 1000))
      eventDataFrame = loadRaw(folderRawLogs+'/'+rawfile)
      fileprefix = time.strftime('%Y-%m-%d', time.localtime(eventDataFrame['timestamp'].values[-1]/1000))
      fileNameEventDate = folderEventData+'/'+'ALL'+'/'+fileprefix+'.parquet'
      mergeDataFrameIntoFilePrep(eventDataFrame, fileNameEventDate)
      print("Loaded from raw:"+rawfile, (int(round(time.time() * 1000))-begintime))

lstALLFiles = dict(datByEventFile)
for (fileNameEventDate, eventDataFrame) in lstALLFiles.items():
   begintime = int(round(time.time() * 1000))
   fileprefix = time.strftime('%Y-%m-%d', time.localtime(eventDataFrame['timestamp'].values[-1]/1000))
   datByEventFile[fileNameEventDate].drop_duplicates(subset=["player", "timestamp", "seq"], inplace=True)
   indDataIsNew = (len(datByEventFile[fileNameEventDate]) != cntByEventFile[fileNameEventDate])
   print(fileNameEventDate+" is new "+str(indDataIsNew))
   if indDataIsNew : # You'll want an indicator for handling if reprocessing for a changed JSON
      for eventName, grpdf in eventDataFrame.groupby('event'):
         if eventName in allEventArgs:
            e = allEventArgs[eventName]
            eventDataFrame = pd.DataFrame(grpdf, columns=e['eventDfCols']).rename(columns=e['eventArgRename']).replace(to_replace=e['eventArgConstmap']).astype(dtype=e['eventArgTypes'])
            mergeDataFrameIntoFilePrep(eventDataFrame, e['destEventPath']+'/'+fileprefix+'.parquet')
         else: lstNotInJSON.add(eventName)
   print("Parsed to event folders:"+fileNameEventDate, (int(round(time.time() * 1000))-begintime))

begintime = int(round(time.time() * 1000))
lstDirsToMake = set(os.path.dirname(key) for (key,value) in cntByEventFile.items() if value == 0 and not os.path.exists(os.path.dirname(key)))
for f in lstDirsToMake: os.makedirs(f)
# write files where data has changed
for (fileNameEventDate, eventDataFrame) in datByEventFile.items():
   eventDataFrame.drop_duplicates(subset=["player", "timestamp", "seq"], inplace=True)
   if len(eventDataFrame) != cntByEventFile[fileNameEventDate] :
      eventDataFrame.to_parquet(fileNameEventDate)
print("Done writing files. ", (int(round(time.time() * 1000))-begintime))


