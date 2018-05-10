# -*- coding: utf-8 -*-
#author: 半熟的韭菜

from websocket import create_connection
import gzip
import time
import os,os.path
import datetime
import json
import csv
import threading

class huobi(threading.Thread):
    NUMBER = 1
    MAX_SIZE = 500000
    def __init__(self,symbol,mType):
        threading.Thread.__init__(self)
        self.symbol = symbol
        self.mType = mType
        self.now = str(datetime.datetime.now())
        self.date = str(self.now)[:10]
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
        NUMBER = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
            
    def getPath(self):
        if(NUMBER < 10):
            path = self.symbol+'/'+self.date+'/'+self.mType+'/huobi_'+self.symbol+'_'+self.mType+'_0'+str(NUMBER)+'.txt'
        else:
            path = self.symbol+'/'+self.date+'/'+self.mType+'/huobi_'+self.symbol+'_'+self.mType+'_'+str(NUMBER)+'.txt'
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
        self.mkdir(self.symbol)
        self.mkdir(self.symbol+'/'+self.date)
        self.mkdir(self.symbol+'/'+self.date+'/'+self.mType)
        self.setNumber()
        
        while(1):
            try:
                ws = create_connection("wss://api.huobipro.com/ws")
                break
            except:
                print('connect ws error,retry...')
                time.sleep(5)
        
        # 订阅 KLine 数据
        #tradeStr="""{"sub": "market.ethusdt.kline.1min","id": "id10"}"""
        tradeStr = ""
        if(self.mType == 'market_depth'):
            tradeStr="""{"sub": "market.%s.depth.step0", "id": "id10"}"""%(self.symbol)
        elif(self.mType == 'trade_detail'):
            tradeStr="""{"sub": "market.%s.trade.detail", "id": "id10"}"""%(self.symbol)
        
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
        else:
            print('mType incorrect')


class huobiConverter(threading.Thread):
    def __init__(self,symbol,mType):
        threading.Thread.__init__(self)
        self.symbol = symbol
        self.mType = mType
        now = str(datetime.datetime.now())
        self.date = str(now)[:10]
        
    def mkdir(self,path):
        '''
        防止目录存在
        '''
        if not os.path.exists(path):
            os.mkdir(path)
            
    def addBracket(self,filename):
        with open(filename, 'r+') as f:
            content = f.read()
            if(content[0] == "{"):
                f.seek(0, 0)
                f.write('[' + content+']')
            
    def readJson(self,filename):
        path = self.symbol+'/'+self.date+'/'+self.mType+'/'+filename
        self.addBracket(path)
        with open(path) as json_file:
            data = json.load(json_file)
            self.writeToCsv(filename,data)
               
    def writeToCsv(self,filename, datas):
        csvFile = self.getPath()
        if(self.mType == 'market_depth'):
            for data in datas:
                if('id' not in data):
                    symbol = data['ch']
                    ts = data['ts']
                    line = []
                    line.append(symbol.split(".")[1])
                    line.append(ts)
                    
                    line.append('bid')
                    for bs in data['tick']['bids']:
                        for bid in bs:
                            line.append(bid)  
                            
                    line.append('ask')
                    for ass in data['tick']['asks']:
                        for ask in ass:
                            line.append(ask)
                    with open(csvFile, "a", newline='') as csv_file:
                        writer = csv.writer(csv_file) 
                        writer.writerow(line)
                    
        elif(self.mType == 'trade_detail'):
            for data in datas:
                if('id' not in data):
                    symbol = data['ch']
                    newSym = symbol.split(".")[1]
                    ts = data['ts']
                    
                    
                    for trade in data['tick']['data']:
                        line = []
                        ts = trade['ts']
                        amount = trade['amount']
                        price = trade['price']
                        direction = trade['direction']
                        line.append(newSym)
                        line.append(ts)
                        line.append(direction)
                        line.append(price)  
                        line.append(amount)  
                        with open(csvFile, "a", newline='') as csv_file:
                            writer = csv.writer(csv_file) 
                            writer.writerow(line)
    
    def getPath(self):
        #csvFile = self.symbol + '/'+filename.split('_')[0] + '_huobi_'+self.symbol+'_'+self.mType+'.csv'
        self.mkdir('output')
        csvFile = 'output/'+self.date + '_huobi_'+self.symbol+'_'+self.mType+'.csv'
        print(csvFile)
        return csvFile   
    
    def run(self):
        for filename in os.listdir(self.symbol+'/'+self.date+'/'+self.mType):
            self.readJson(filename)             
                
if __name__ == '__main__':
    downloadThreads = []   
    convertThreads = []
    ethusdtSym = 'ethusdt'
    btcusdtSym = 'btcusdt'
    bchusdtSym = 'bchusdt'
    etcusdtSym ='etcusdt'
    ltcusdtSym ='ltcusdt'
    
    marketDepth = 'market_depth'
    tradeDetail = 'trade_detail'
    
    downloadThreads.append(huobi(ethusdtSym, marketDepth))
    downloadThreads.append(huobi(btcusdtSym, marketDepth))
    downloadThreads.append(huobi(bchusdtSym, marketDepth))
    downloadThreads.append(huobi(etcusdtSym, marketDepth))
    downloadThreads.append(huobi(ltcusdtSym, marketDepth))
    
    downloadThreads.append(huobi(ethusdtSym, tradeDetail))
    downloadThreads.append(huobi(btcusdtSym, tradeDetail))
    downloadThreads.append(huobi(bchusdtSym, tradeDetail))
    downloadThreads.append(huobi(etcusdtSym, tradeDetail))
    downloadThreads.append(huobi(ltcusdtSym, tradeDetail))
    
    convertThreads.append(huobiConverter(ethusdtSym, marketDepth))
    convertThreads.append(huobiConverter(btcusdtSym, marketDepth))
    convertThreads.append(huobiConverter(bchusdtSym, marketDepth))
    convertThreads.append(huobiConverter(etcusdtSym, marketDepth))
    convertThreads.append(huobiConverter(ltcusdtSym, marketDepth))
    
    convertThreads.append(huobiConverter(ethusdtSym, tradeDetail))
    convertThreads.append(huobiConverter(btcusdtSym, tradeDetail))
    convertThreads.append(huobiConverter(bchusdtSym, tradeDetail))
    convertThreads.append(huobiConverter(etcusdtSym, tradeDetail))
    convertThreads.append(huobiConverter(ltcusdtSym, tradeDetail))
    
# =============================================================================
#     for dt in downloadThreads:
#         dt.start()
# =============================================================================
        
    for ct in convertThreads:
        ct.start()
    
    for ct in convertThreads:
        ct.join()
    
# =============================================================================
#     ethusdt.start()
#     btcusdt.start()
#     bchusdt.start()
#     etcusdt.start()
#     ltcusdt.start()
# =============================================================================

# =============================================================================
#     huobiConverter(ethusdtSym, mType).run()
#     huobiConverter(btcusdtSym, mType).run()
#     huobiConverter(bchusdtSym, mType).run()
#     huobiConverter(etcusdtSym, mType).run()
#     huobiConverter(ltcusdtSym, mType).run()
# =============================================================================
    
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
    
        
            

