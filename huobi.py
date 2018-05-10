# -*- coding: utf-8 -*-
#author: 半熟的韭菜

from websocket import create_connection
import gzip
import time
import os,os.path
import datetime
import json
import csv
#from multiprocessing import Process
import threading

class huobi(threading.Thread):
    MAX_SIZE = 50000000
    def __init__(self,symbols,mType):
        threading.Thread.__init__(self)
        self.symbols = symbols
        self.mType = mType
        date = self.getDateStr()
        self.NUMBER = len([name for name in os.listdir(date) if os.path.isfile(os.path.join(date, name))])
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)
            self.NUMBER = 1

            
    def increment(self):
        self.NUMBER = self.NUMBER + 1
        
    def getDateStr(self):
        now = str(datetime.datetime.now())
        date = str(now)[:10]
        return date
            
    def getPath(self):
        date = self.getDateStr()
        self.mkdir(date)
        
        if(self.NUMBER < 10):
            path = date+'/huobi_'+'_'+self.mType+'_0'+str(self.NUMBER)+'.txt'
        else:
            path = date+'/huobi_'+'_'+self.mType+'_'+str(self.NUMBER)+'.txt'
        return path

    def writeToJson(self, content):
        path = self.getPath() 
        # Write JSON file
        if(os.path.exists(path)):
            if(os.stat(path).st_size <= self.MAX_SIZE):
                with open(path,'a') as f:
                    print('wrote to {0}'.format(str(path)))
                    f.write(","+content)
            else:
                self.increment()
                self.writeToJson(content)
                
        else:
            with open(path,'a') as f:
                print('wrote to {0}'.format(str(path)))
                f.write(content)


    def run(self):
        
        
        while(1):
            try:
                ws = create_connection("wss://api.huobipro.com/ws")
                break
            except:
                print('connect ws error,retry...')
                time.sleep(5)
        
        # 订阅 KLine 数据
        #tradeStr="""{"sub": "market.ethusdt.kline.1min","id": "id10"}"""
        for symbol in self.symbols:
            tradeStr = ""
            if(self.mType == 'market_depth'):
                tradeStr="""{"sub": "market.%s.depth.step0", "id": "id10"}"""%(symbol)
            elif(self.mType == 'trade_detail'):
                tradeStr="""{"sub": "market.%s.trade.detail", "id": "id10"}"""%(symbol)
            
            # 请求 KLine 数据
            # tradeStr="""{"req": "market.ethusdt.kline.1min","id": "id10", "from": 1513391453, "to": 1513392453}"""
            
            #订阅 Market Depth 数据
            # tradeStr="""{"sub": "market.ethusdt.depth.step5", "id": "id10"}"""
            
            #请求 Market Depth 数据
            # tradeStr="""{"req": "market.ethusdt.depth.step5", "id": "id10"}"""
            
            #订阅 Trade Detail 数据
            # tradeStr="""{"sub": "market.ethusdt.trade.detail", "id": "id10"}"""
            
            #请求 Trade Detail 数据
            # tradeStr="""{"req": "market.ethusdt.trade.detail", "id": "id10"}"""
            
            #请求 Market Detail 数据
            # tradeStr="""{"req": "market.ethusdt.detail", "id": "id12"}"""
            if(tradeStr):
                ws.send(tradeStr)
            else:
                print('mType incorrect')
        while(1):
            compressData=ws.recv()
            result=gzip.decompress(compressData).decode('utf-8')
            if result[:7] == '{"ping"':
                ts=result[8:21]
                pong='{"pong":'+ts+'}'
                ws.send(pong)
                ws.send(tradeStr)
            else:
                #print(result)
                self.writeToJson(result)
            #time.sleep(1)


class huobiConverter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
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
    downloadThreads = []   
    convertThreads = []
    symbols = ['ethusdt','btcusdt','bchusdt','etcusdt','ltcusdt','eosusdt','xrpusdt','omgusdt','dashusdt','zecusdt','iotausdt','adausdt','steemusdt','bchbtc','ethbtc','ltcbtc','etcbtc','eosbtc','omgbtc','xrpbtc','dashbtc','zecbtc','iotabtc','adabtc','steembtc']
    marketDepth = 'market_depth'
    tradeDetail = 'trade_detail'
    
    a=huobi(symbols, marketDepth)
    b=huobi(symbols, tradeDetail)

    
    downloadThreads.append(huobi(symbols, marketDepth))
    downloadThreads.append(huobi(symbols, tradeDetail))
    convertThreads.append(huobiConverter())
    
# =============================================================================
#     for dt in downloadThreads:
#         dt.start()
# =============================================================================
        
    for ct in convertThreads:
        ct.start()
    
    for ct in convertThreads:
        ct.join()
        

