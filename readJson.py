import csv
import json
import os

def readJson(symbol, kind, filename):
    with open(symbol+'/'+filename) as json_file:
        data = json.load(json_file)
        writeToCsv(symbol, kind, filename,data)
           
def writeToCsv(symbol, kind, filename, datas):
    csvFile = getPath(symbol, kind, filename)
    for data in datas:
        symbol = data['rep']
        ts = data['ts']
        line = []
        line.append(symbol.split(".")[1])
        line.append(ts)
        
        line.append('bid')
        for bs in data['data']['bids']:
            for bid in bs:
                line.append(bid)  
                
        line.append('ask')
        for ass in data['data']['asks']:
            for ask in ass:
                line.append(ask)
        
        print(line)
        with open(csvFile, "a", newline='') as csv_file:
            writer = csv.writer(csv_file) 
            writer.writerow(line)

def getPath(symbol, kind, filename):
    csvFile = symbol + '/'+filename.split('_')[0] + '_'+symbol+'_'+kind+'.csv'
    print(csvFile)
    return csvFile

def main(symbol,kind):
    for filename in os.listdir(symbol):
        readJson(symbol, kind, filename)

if __name__ == '__main__':
    symbol = 'ethusdt'
    kind = 'market'
    main(symbol, kind)