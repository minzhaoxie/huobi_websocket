import os.path

from datetime import datetime
import csv
import fnmatch

class sumCsv:
    def __init__(self):
        now = str(datetime.now())
        self.date = str(now)[:10]
        
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)
            
    def readCsv(self, csvPath):
        with open(csvPath, 'r') as csvFile:
            reader = csv.reader(csvFile)
            sell = dict()
            buy = dict()
            for x in range(0,24):
                sell[str(x)] = 0
                buy[str(x)] = 0
            for row in reader:
                direction = row[2]
                if(direction == 'sell'):
                    sell[self.getHour(row[1])] = sell[self.getHour(row[1])] + float(row[4])
                elif(direction == 'buy'):
                    buy[self.getHour(row[1])] = buy[self.getHour(row[1])] + float(row[4])
            
            #print(sell.values())
            sellLine = [row[0], 'sell']
            buyLine = [row[0], 'buy']
            for value in sell.values():
                sellLine.append(value)
            for value in buy.values():
                buyLine.append(value)
                
            self.writeToCsv(sellLine)
            self.writeToCsv(buyLine)
    
    def getHour(self,ts):
        return str(datetime.utcfromtimestamp(int(ts)/1e3).hour)
    
    def writeToCsv(self,line):
        self.mkdir('output')
        path = '2018-05-13_huobi_trade_detail_result.csv'
        if os.path.isfile(path):
            with open(path, "a", newline='') as csv_file:
                        writer = csv.writer(csv_file) 
                        writer.writerow(line)
        else:
            header = ['pair','direction']
            for x in range(0,24):
                header.append(str(x))
            with open(path, "a", newline='') as csv_file:
                        writer = csv.writer(csv_file) 
                        writer.writerow(header)
                        writer.writerow(line)
        
    def getLine(self,dic):
        line = []
        for value in dic.values():
            line.append(value)
        
        return line
    
    def run(self):
        print()
        

if __name__ == '__main__':
    sumCsv = sumCsv()
    DIR = 'output/'
    print (len(fnmatch.filter(os.listdir(DIR), '*_trade_detail.csv')))
    for file in os.listdir(DIR):
        if fnmatch.fnmatch(file, '*_trade_detail.csv'):
            sumCsv.readCsv(DIR+file)
    #sumCsv.readCsv(path)
    #print (str(datetime.utcfromtimestamp(int('1526219392873')/1e3).hour))