import pandas as pd
import json
import argparse
from collections import OrderedDict
import numpy as np
from GenAlg import GenAlg
import ast
import Classes
from Classes import *
import collections
from collections import Counter
import jsonschema
from jsonschema import Draft7Validator
import re

jsonData = []
data = {}

skiprows = 4

idColumnsList = ["scanGeomID", "pulseID", "txDelayID", "txAperID", "gainID", "rxAperID", "rxDelayID", "echoFilterID"]

def arrangeJsonStr(jsonStr):
   newStr = ""
   newLine = "\n"
   for line in jsonStr.split("\n"):
       if line.strip().startswith("["):
         newLine = ""
       elif line.endswith("]"):
         newLine = "\n"
       newStr += line + newLine

   jsonStr = newStr
   newStr = ""
   for line in jsonStr.split("\n"):
       if line.strip().startswith("[") and line.strip().endswith("]"):
           spaces = spaces = len(line) - len(line.lstrip(' '))
           line = (' ' * spaces) + re.sub("\\s+", "", line)
       newStr += line + '\n'
   return newStr

def getTwoLevelCols(sheetName):
    testDesign = xls.parse(sheet_name=sheetName, header=[0,1], skiprows=skiprows)
    tsys = pd.DataFrame(testDesign)
    tsys = tsys.fillna(method='ffill')
    colCount = dict(Counter(tsys.columns.get_level_values(0)))
    cols = []
    colLvl0 = list(tsys.columns.get_level_values(0))
    colLvl1 = list(tsys.columns.get_level_values(1))

    for col in range(len(tsys.columns.get_level_values(0))):
        if col == 0:
            continue
        if "unnamed" in colLvl1[col].lower():
            cols.append(colLvl0[col])
        else:
            cols.append(colLvl1[col])
    return (cols, colCount, colLvl0[1:])


def typeConversion(type, value):
    if type == "vector(vector(ureal))" or type == "vector(vector(real))" or type == "vector(vector(uint))":
        value = value.replace("(","[").replace(")","]")
        value = ast.literal_eval(value)
    elif type == "vector(real)" or type == "vector(uint)" or type == "vector(ureal)":
        value = value.replace("(","[").replace(")","]")
        value = ast.literal_eval(value)
    elif type == "bool":
        if value == "true" or value == 1.0 or value == 1:
            value = True
        elif value == "false" or value == 0.0 or value == 0:
            value = False
    return value

def getType(dtype, df):
    dfCols = df.columns.to_list()
    for i in range(len(dtype)):
        if dtype[i] == "string":
            df[dfCols[i]] = df[dfCols[i]].astype(np.str)
        elif dtype[i] == "bool":
            df[dfCols[i]] = df[dfCols[i]].astype(np.bool)
        elif not dtype[i] == "uint" or dtype[i] == "int":
            val = typeConversion(dtype[i], list(df.values[0])[i])
            df[dfCols[i]].values[0] = val
        else:
            df[dfCols[i]] = df[dfCols[i]].astype(np.int64)
    return df

def dumpJson(df, name):
    columns = [str(k) for k in df.columns]
    jsonData.append({name:[OrderedDict(zip(columns, row)) for row in df.values]})
    with open(args.outputFile, 'w') as outfile:
        json.dump(jsonData, outfile, indent=4)

def arrangeData(df, name):
    columns = [str(k) for k in df.columns]
    if name == "":
        return([OrderedDict(zip(columns, row)) for row in df.values])
    else:
        return({name:[OrderedDict(zip(columns, row)) for row in df.values],})

parser = argparse.ArgumentParser(description='Excel to JSON Converter')
parser.add_argument('-i', dest="inputFile", required=True, help='Specify Excel file as input')
parser.add_argument('-o',dest="outputFile", required=True, help='Specify output json file')
parser.add_argument('--schema',dest="schemaFile", required=True, help='Specify Schema to validate dumped json file')
parser.add_argument('--scanid',dest="scanId", required=True, type=str, help='Specify scanId, Like: "AS"')
parser.add_argument('--cfgid',dest="cfgId", required=False, type=str, default="C1", help='Specify config Id,')

args = parser.parse_args()

xls = pd.ExcelFile(args.inputFile)
# print(len(xls.sheet_names))

scanDesign = xls.parse(sheet_name="ScanDesign", usecols=range(1,31), skiprows=skiprows, nrows=47)
config = xls.parse(sheet_name="Config", usecols=range(1,21), skiprows=skiprows)
geometry = xls.parse(sheet_name="ScanGeom", skiprows=skiprows, nrows=44)
txDelay = xls.parse(sheet_name="TXDelay", usecols=range(1,7), skiprows=skiprows)
txApr = xls.parse(sheet_name="TXAper", usecols=range(1,7), skiprows=skiprows)
pulse = xls.parse(sheet_name="Pulse", usecols=range(1,8), skiprows=skiprows)
Gain = xls.parse(sheet_name="Gain", usecols=range(1,9), skiprows=skiprows)
rxDelay = xls.parse(sheet_name="RXDelay", usecols=range(1,9), skiprows=skiprows)
rxApr = xls.parse(sheet_name="RXAper", usecols=range(1,11), skiprows=skiprows)
echo = xls.parse(sheet_name="EchoFilter", usecols=range(1,13), skiprows=skiprows)

dfGeo = pd.DataFrame(geometry)
dfConfig = pd.DataFrame(config)
dfSys = pd.DataFrame(scanDesign)
dfTXDelay = pd.DataFrame(txDelay)
dfTXApr = pd.DataFrame(txApr)
dfPluse = pd.DataFrame(pulse)
dfGain = pd.DataFrame(Gain)
dfRXDelay = pd.DataFrame(rxDelay)
dfRXApr = pd.DataFrame(rxApr)
dfEcho = pd.DataFrame(echo)

if (list(dfSys["scanDesignID"]).count(args.scanId)) > 1:
    raise Exception("Exception : ScanDesignID '{}' cannot be specified more than once in the '{}'".format(args.scanId, args.inputFile))
if args.scanId not in dfSys["scanDesignID"].to_list():
    raise Exception("Entered scanID '{}' not present among available ScanDesignIDs".format(args.scanId))

dfSys = dfSys.fillna(method='ffill')
dfSys = dfSys.drop_duplicates()
df = dfSys.loc[dfSys['scanDesignID'] == args.scanId]

subFrameList = []
eventList = []
scanGeomDict = OrderedDict()
pulseDict = OrderedDict()
txDelay = OrderedDict()
txAperture = OrderedDict()
gainDict = OrderedDict()
rxAperture = OrderedDict()
rxDelay = OrderedDict()
echoFilter = OrderedDict()
configObj = None
scanDesignObj = None
scanObj = None

#ScanGeom
scanGeomIDs = set(list(df["scanGeomID"]))
pulseIDs = set(list(df["pulseID"]))
txDelayIDs = set(list(df["txDelayID"]))
txApertureIDs = set(list(df["txAperID"]))
gainIDs = set(list(df["gainID"]))
rxDelayIDs = set(list(df["rxDelayID"]))
rxApertureIDs = set(list(df["rxAperID"]))
echoFilterIDs = set(list(df["echoFilterID"]))


# Config
dfConfig = dfConfig.fillna(method="ffill")
dfConfig = dfConfig.drop_duplicates()
dfConfig.drop(index=0, inplace=True)

#Reading the data type
config = xls.parse(sheet_name="Config", usecols=range(1,21), header=None).iloc[0]
configType = config.to_list()

dfTemp = dfConfig.loc[dfConfig['configID'] == args.cfgId].copy()
if not dfTemp.empty:
    rowDict = OrderedDict()
    getType(configType, dfTemp)
    for i in range(len(dfTemp.columns)):
        # if colsCount[colsLevel0[i]] == 1:
        rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
    configObj = Config.from_dict(rowDict)

# ScanGeom
dfGeo = dfGeo.fillna(method="ffill")
dfGeo = dfGeo.drop_duplicates()
dfGeo.drop(index=0, inplace=True)

#Reading the data type
geo = xls.parse(sheet_name="ScanGeom", usecols=range(1,17), header=None).iloc[0]
geoType = geo.to_list()

cols, colsCount, colsLevel0 = getTwoLevelCols("ScanGeom")
for sid in scanGeomIDs:
    dfTemp = dfGeo.loc[dfGeo['scanGeomID'] == sid].copy()
    dfTemp.drop(columns=[dfTemp.columns[0]], inplace=True)
    columnsList = list(dfTemp.columns)
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(geoType, dfTemp)
        for i in range(len(cols)):
            if colsCount[colsLevel0[i]] == 1:
                rowDict[cols[i]] = dfTemp[columnsList[i]].values.tolist()[0]

        #Depth: Multi Column
        depthDict = OrderedDict()
        for i in range(5,9):
            depthDict[cols[i]] = dfTemp[columnsList[i]].values.tolist()[0]
        rowDict["depth"] = depthDict
        scanGeomDict[sid] = ScanGeom.from_dict(rowDict)

# Pluse
dfPluse = dfPluse.fillna(method="ffill")
dfPluse = dfPluse.drop_duplicates()
dfPluse.drop(index=0, inplace=True)

#Reading the data type
pulDf = xls.parse(sheet_name="Pulse", usecols=range(1,8), header=None).iloc[0]
pulseType = pulDf.to_list()

for sid in pulseIDs:
    dfTemp = dfPluse.loc[dfPluse['pulseID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(pulseType, dfTemp)
        for i in range(len(dfTemp.columns)):
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        if rowDict["genAlg"] == 'SIMPLE':
            pulse = GenAlg.pulseGen(configObj.tx_clk_freq, rowDict["pulseCenterFreq"], rowDict["pulseCycles"], rowDict["dutyCycle"])
            rowDict['pulse'] = pulse
        pulseDict[sid] = Pulse.from_dict(rowDict)


# TXDelay
dfTXDelay = dfTXDelay.fillna(method="ffill")
dfTXDelay = dfTXDelay.drop_duplicates()
dfTXDelay.drop(index=0, inplace=True)

#Reading the data type
txDDf = xls.parse(sheet_name="TXDelay", usecols=range(1,7), header=None).iloc[0]
txDType = txDDf.to_list()

for sid in txDelayIDs:
    dfTemp = dfTXDelay.loc[dfTXDelay['txDelayID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(txDType, dfTemp)
        for i in range(len(dfTemp.columns)):
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        txDelay[sid] = TXDelay.from_dict(rowDict)

# TXAper
dfTXApr = dfTXApr.fillna(method="ffill")
dfTXApr = dfTXApr.drop_duplicates()
dfTXApr.drop(index=0, inplace=True)

#Reading the data type
txADf = xls.parse(sheet_name="TXAper", usecols=range(1,7), header=None).iloc[0]
txAType = txADf.to_list()

for sid in txApertureIDs:
    dfTemp = dfTXApr.loc[dfTXApr['txAperID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(txAType, dfTemp)
        for i in range(len(dfTemp.columns)):
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        txAperture[sid] = TXAper.from_dict(rowDict)

# Gain
dfGain = dfGain.fillna(method="ffill")
dfGain = dfGain.drop_duplicates()
dfGain.drop(index=0, inplace=True)

#Reading the data type
gain = xls.parse(sheet_name="Gain", usecols=range(1,9), header=None).iloc[0]
gainType = gain.to_list()
for sid in gainIDs:
    dfTemp = dfGain.loc[dfGain['gainID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(gainType, dfTemp)
        for i in range(len(dfTemp.columns)):
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        gainDict[sid] = Classes.Gain.from_dict(rowDict)

# RXAper
dfRXApr = dfRXApr.fillna(method="ffill")
dfRXApr = dfRXApr.drop_duplicates()
dfRXApr.drop(index=0, inplace=True)

#Reading the data type
rxADf = xls.parse(sheet_name="RXAper", usecols=range(1,11), header=None).iloc[0]
rxAType = rxADf.to_list()

for sid in rxApertureIDs:
    dfTemp = dfRXApr.loc[dfRXApr['rxAperID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(rxAType, dfTemp)
        for i in range(len(dfTemp.columns)):
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        rxAperture[sid] = RXAper.from_dict(rowDict)

# RXDelay
dfRXDelay = dfRXDelay.fillna(method="ffill")
dfRXDelay = dfRXDelay.drop_duplicates()
dfRXDelay.drop(index=0, inplace=True)

#Reading the data type
rxDelay = xls.parse(sheet_name="RXDelay", usecols=range(1,8), header=None).iloc[0]
rxDelayType = rxDelay.to_list()

for sid in rxDelayIDs:
    dfTemp = dfRXDelay.loc[dfRXDelay['rxDelayID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(rxDelayType, dfTemp)
        for i in range(len(dfTemp.columns)):
            # if colsCount[colsLevel0[i]] == 1:
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        rxDelay[sid] = RXDelay.from_dict(rowDict)


# echoFilter
dfEcho = dfEcho.fillna(method="ffill")
dfEcho = dfEcho.drop_duplicates()
dfEcho.drop(index=0, inplace=True)

#Reading the data type
echoFilter = xls.parse(sheet_name="EchoFilter", usecols=range(1,12), header=None).loc[0]
echoFilterType = echoFilter.to_list()

for sid in echoFilterIDs:
    dfTemp = dfEcho.loc[dfEcho['echoFilterID'] == sid].copy()
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(echoFilterType, dfTemp)
        for i in range(len(dfTemp.columns)):
            # if colsCount[colsLevel0[i]] == 1:
            rowDict[dfTemp.columns[i]] = dfTemp[dfTemp.columns[i]].values.tolist()[0]
        echoFilter[sid] = EchoFilter.from_dict(rowDict)

#SubFrames
subFrames = df["subFrameID"]
subFrames = set(list(subFrames))

#Reading the data type
subframeEx = xls.parse(sheet_name="ScanDesign", usecols=range(1,21), header=None).iloc[0]
subFrameType = subframeEx.to_list()

#print(subFrameType)
# for subFrameID in subFrames:
#     dfTemp = df.ix[df['subFrameID'] == subFrameID]
#     if not dfTemp.empty:
#         print (dfTemp)
pd.set_option('display.max_columns', 30)

cols, colsCount, colsLevel0 = getTwoLevelCols("ScanDesign")
for subFrameID in subFrames:
    dfTemp = df.loc[df['subFrameID'] == subFrameID].copy()
    columnsList = list(dfTemp.columns)
    if not dfTemp.empty:
        rowDict = OrderedDict()
        getType(subFrameType, dfTemp)
        for i in range(len(cols)):
            if colsCount[colsLevel0[i]] == 1 and ("ID" not in colsLevel0[i] or "subFrameID" in colsLevel0[i]) :
                rowDict[cols[i]] = dfTemp[cols[i]].values.tolist()[0]

        rowDict["apodType"] = ast.literal_eval(rowDict["apodType"].replace("(",'["').replace(")",'"]').replace(",", '","'))

        #SubMode: Multi Column
        subModeDict = OrderedDict()
        for i in range(4,13):
            subModeDict[cols[i]] = dfTemp[columnsList[i]].values.tolist()[0]

        #EventIndex: Multi Column
        eventIndexDict = OrderedDict()
        for i in range(13,18):
            eventIndexDict[cols[i]] = dfTemp[columnsList[i]].values.tolist()[0]

        eventObj = Event.from_dict(eventIndexDict)
        subModeObj = SubMode.from_dict(subModeDict)

        scanGeomList = []
        pulseList = []
        txDelayList = []
        txAperList = []
        gainList = []
        rxAperList = []
        rxDelayList = []
        echoFilterList = []

        for colName in idColumnsList:
            lst = set(list(dfTemp[colName]))
            for idObj in lst:
                try:
                    if colName == "scanGeomID":
                        scanGeomList.append(scanGeomDict[idObj])

                    elif colName == "pulseID":
                        pulseList.append(pulseDict[idObj])

                    elif colName == "txDelayID":
                        txDelayList.append(txDelay[idObj])

                    elif colName == "txAperID":
                        txAperList.append(txAperture[idObj])

                    elif colName == "gainID":
                        gainList.append(gainDict[idObj])

                    elif colName == "rxAperID":
                        rxAperList.append(rxAperture[idObj])

                    elif colName == "rxDelayID":
                        rxDelayList.append(rxDelay[idObj])

                    elif colName == "echoFilterID":
                        echoFilterList.append(echoFilter[idObj])
                except Exception as e:
                    print ("Warning : {} - {} Not Found ".format(colName, idObj))

        subFrameObj = Subframe(rowDict["subFrameID"], eventObj, rowDict["PRI"], rowDict["RI"], [subModeObj], scanGeomList, pulseList, txDelayList, txAperList, gainList,
                               rxDelayList, rxAperList, rowDict["apodType"], echoFilterList, rowDict["rxSynthesisType"])

        subFrameList.append(subFrameObj)

scanDesignID = set(list(df["scanDesignID"])).pop()
description = set(list(df["description"])).pop()
SRI = set(list(df["SRI"])).pop()

scanDesign = ScanDesign(scanDesignID, description, SRI, subFrameList)
scanObj = Scan(configObj, scanDesign)

jsonStr = json.dumps(scanObj.to_dict(), indent=4)
jsonStr = arrangeJsonStr(jsonStr)

with open(args.outputFile, 'w') as f:
    f.write(jsonStr)

print ("JSON {} dumped".format(args.outputFile))

jsonData = None
jsonSchema = None
with open(args.outputFile, 'r') as f:
       jsonData = json.load(f)

with open(args.schemaFile, 'r') as f2:
       jsonSchema = json.load(f2)

v = Draft7Validator(jsonSchema)
errors = sorted(v.iter_errors(jsonData), key=lambda e: e.path)
if errors:
    print("Schema Validation Failed with following errors:")
    print('\n'.join(
        'Error: %s %s' % (list(error.path), error.message) for error in errors
    ))
else:
    print("Schema Validation Successfully Completed")
