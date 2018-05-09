# -*- coding: utf-8 -*-
#author: 半熟的韭菜

from websocket import create_connection
import gzip
import time
import os,os.path
import datetime
import json
import csv
class huobi():
    NUMBER = 1
    MAX_SIZE = 50000
    def __init__(self,symbol,mType):
        self.symbol = symbol
        self.mType = mType
        
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)

    def increment(self):
        global NUMBER
        NUMBER = NUMBER+1
    
    def setNumber(self):
        DIR = self.symbol
        global NUMBER
        NUMBER = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])+1
            
    def getPath(self):
        now = str(datetime.datetime.now())
        symbol = self.symbol
        kind = self.mType
        self.mkdir(symbol)
        name = str(now)[:10]
        if(NUMBER < 10):
            path = symbol+'/'+name+'_huobi_'+symbol+'_'+kind+'_0'+str(NUMBER)+'.txt'
        else:
            path = symbol+'/'+name+'_huobi_'+symbol+'_'+kind+'_'+str(NUMBER)+'.txt'
        return path

    def writeToJson(self, content):
        path = self.getPath() 
        # Write JSON file
        if(os.path.exists(path)):
            if(os.stat(path).st_size <= self.MAX_SIZE):
                with open(path,'a') as f:
                    print(content)
                    f.write(","+content)
            else:
                self.increment()
                self.writeToJson(content)
                
        else:
            with open(path,'a') as f:
                print(content)
                f.write(content)


    def run(self):
        self.mkdir(self.symbol)
        #self.setNumber()
        
        while(1):
            try:
                ws = create_connection("wss://api.huobipro.com/ws")
                break
            except:
                print('connect ws error,retry...')
                time.sleep(5)
        
        # 订阅 KLine 数据
        #tradeStr="""{"sub": "market.ethusdt.kline.1min","id": "id10"}"""
        tradeStr="""{"req": "market.%s.%s.step0", "id": "id10"}"""%(self.symbol, self.mType)
        
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
        
        ws.send(tradeStr)

        compressData=ws.recv()
        result=gzip.decompress(compressData).decode('utf-8')
        if result[:7] == '{"ping"':
            ts=result[8:21]
            pong='{"pong":'+ts+'}'
            ws.send(pong)
            ws.send(tradeStr)
        else:
            self.writeToJson(result) 


class huobiConverter:
    def __init__(self,symbol,mType):
        self.symbol = symbol
        self.mType = mType
        
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)
            
    def addBracket(self,filename):
        with open(filename, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('[' + content+']')
            
    def readJson(self,filename):
        path = self.symbol+'/'+filename
        self.addBracket(path)
        with open(path) as json_file:
            data = json.load(json_file)
            self.writeToCsv(filename,data)
               
    def writeToCsv(self,filename, datas):
        csvFile = self.getPath(filename)
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
    
    def getPath(self,filename):
        #csvFile = self.symbol + '/'+filename.split('_')[0] + '_huobi_'+self.symbol+'_'+self.mType+'.csv'
        self.mkdir('output')
        csvFile = 'output/'+filename.split('_')[0] + '_huobi_'+self.symbol+'_'+self.mType+'.csv'
        print(csvFile)
        return csvFile   
    
    def run(self):
        for filename in os.listdir(self.symbol):
            self.readJson(filename)             
                
if __name__ == '__main__':
    threads = []   
    symbol1 = 'ethusdt'
    symbol2 = 'btcusdt'
    symbol3 = 'bchusdt'
    mType = 'depth'
    ethusdt = huobi(symbol1, mType)
    btcusdt = huobi(symbol2, mType)
    bchusdt = huobi(symbol3, mType)
    i = 10
    while(i):
        ethusdt.run()
        btcusdt.run()
        bchusdt.run()
        i = i -1
    
    huobiConverter(symbol1, mType).run()
    huobiConverter(symbol2, mType).run()
    huobiConverter(symbol3, mType).run()
    
# =============================================================================
#     try:
#         ethusdt.start()
#         threads.append(ethusdt)  
#         btcusdt.start()
#         threads.append(btcusdt)  
#         bchusdt.start()
#         threads.append(bchusdt)  
#     except KeyboardInterrupt:
#         for t in threads:  
#             # 等待线程完成  
#             t.join()  
# =============================================================================
# =============================================================================
#     try:
#         ethusdt.start()
#         btcusdt.start()
#     except KeyboardInterrupt:
#         print ('Interrupted')
#         ethusdt.addBracket(ethusdt.getPath())
#         btcusdt.addBracket(btcusdt.getPath())
#         ethusdt.join()
#         btcusdt.join()
#         try:
#             sys.exit(0)
#         except SystemExit:
#             os._exit(0)
# =============================================================================
# =============================================================================
#     converter = huobiConverter(symbol1, mType)
#     converter.run()
# =============================================================================
    
        
            

