import os,os.path
import datetime
import json
import csv
from multiprocessing import Process
#import threading


class huobiConverter(Process):
    def __init__(self):
        #threading.Thread.__init__(self)
        Process.__init__(self)
        now = str(datetime.datetime.now())
        self.date = str(now)[:10]
        
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)
            
    def addBracket(self,filename):
        print('\n workind on : \n')
        print(filename)
        with open(filename, 'r+') as f:
            content = f.read()
            with open(self.date+'/tmp.txt', 'w') as t:
                if(content[0] == "{"):
                    t.seek(0, 0)
                    t.write('[' + content+']')
                else:
                    t.write(content)
                t.close()
            f.close()
            
    def readJson(self,filename):
        path = self.date+'/'+filename
        self.addBracket(path)
        with open(self.date+'/tmp.txt') as json_file:
            data = json.load(json_file)
            self.writeToCsv(filename,data)
            json_file.close()
        os.remove(self.date+'/tmp.txt')
        
            
               
    def writeToCsv(self,filename, datas):
        for data in datas:
            if('id' not in data):
                ch = data['ch']
                if('depth' in ch):
                    symbol = ch.split(".")[1]
                    ts = data['ts']
                    line = []
                    line.append(symbol)
                    line.append(ts)
                    line.append('bid')
                    for bs in data['tick']['bids']:
                        for bid in bs:
                            line.append(bid)  
                            
                    line.append('ask')
                    for ass in data['tick']['asks']:
                        for ask in ass:
                            line.append(ask)
                    
                    csvFile = self.getPath(symbol,'market_depth')
                    with open(csvFile, "a", newline='') as csv_file:
                        writer = csv.writer(csv_file) 
                        writer.writerow(line)
                elif('trade.detail' in ch):
                    symbol = ch.split(".")[1]
                    ts = data['ts']
                    for trade in data['tick']['data']:
                        line = []
                        ts = trade['ts']
                        amount = trade['amount']
                        price = trade['price']
                        direction = trade['direction']
                        line.append(symbol)
                        line.append(ts)
                        line.append(direction)
                        line.append(price)  
                        line.append(amount) 
                        csvFile = self.getPath(symbol,'trade_detail')
                        with open(csvFile, "a", newline='') as csv_file:
                            writer = csv.writer(csv_file) 
                            writer.writerow(line)

                    
    
    def getPath(self,symbol,mType):
        #csvFile = self.symbol + '/'+filename.split('_')[0] + '_huobi_'+self.symbol+'_'+self.mType+'.csv'
        self.mkdir('output')
        csvFile = 'output/'+self.date + '_huobi_'+symbol+'_'+mType+'.csv'
        print(csvFile)
        return csvFile   
    
    def run(self):
        for filename in os.listdir(self.date):
            self.readJson(filename)             
                
if __name__ == '__main__':
    convertThreads = []
    symbols = ['ethusdt','btcusdt','bchusdt','etcusdt','ltcusdt','eosusdt','xrpusdt','omgusdt','dashusdt','zecusdt','iotausdt','adausdt','steemusdt','bchbtc','ethbtc','ltcbtc','etcbtc','eosbtc','omgbtc','xrpbtc','dashbtc','zecbtc','iotabtc','adabtc','steembtc']
    marketDepth = 'market_depth'
    tradeDetail = 'trade_detail'
    
    convertThreads.append(huobiConverter())
    
    for ct in convertThreads:
        ct.start()
    
    for ct in convertThreads:
        ct.join()
        

