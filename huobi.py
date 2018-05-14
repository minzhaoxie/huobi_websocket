# -*- coding: utf-8 -*-
#author: Max

from websocket import create_connection
import gzip
import time
import os,os.path
import datetime
#from multiprocessing import Process
import threading
import fnmatch

class huobi(threading.Thread):
    #100MB
    MAX_SIZE = 100000000
    def __init__(self,symbols,mType):
        threading.Thread.__init__(self)
        #Process.__init__(self)
        self.symbols = symbols
        self.mType = mType
        date = self.getDateStr()
        self.mkdir(date)
        self.NUMBER = len(fnmatch.filter(os.listdir(date), '*'+mType+'*'))+1
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
        now = str(datetime.datetime.utcnow())
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
            try:
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
            except:
                
                with open('log.txt','a') as log:
                    print("\n\n Error occured, disconnected!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n")
                    log.write(str(datetime.datetime.now()))
                    log.write("\n\n Error occured, disconnected!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n")
                log.close()
                self.run()
                break
       
                
if __name__ == '__main__':
    downloadThreads = []   
    symbols = ['ethusdt','btcusdt','bchusdt','etcusdt','ltcusdt','eosusdt','xrpusdt','omgusdt','dashusdt','zecusdt','iotausdt','adausdt','steemusdt','bchbtc','ethbtc','ltcbtc','etcbtc','eosbtc','omgbtc','xrpbtc','dashbtc','zecbtc','iotabtc','adabtc','steembtc']
    marketDepth = 'market_depth'
    tradeDetail = 'trade_detail'
    
    
    downloadThreads.append(huobi(symbols, marketDepth))
    downloadThreads.append(huobi(symbols, tradeDetail))
    
    for dt in downloadThreads:
        dt.start()
        
