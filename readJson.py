import csv
import json
def readJson(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
        writeToCsv(filename,data)
           
def writeToCsv(filename, datas):
    filename.replace('.csv','.txt')
    csvFile = filename[:-4]+'.csv'
    print(csvFile)
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


if __name__ == '__main__':
    path = 'ethusdt/2018-05-04_ethusdt.txt'
    readJson(path)