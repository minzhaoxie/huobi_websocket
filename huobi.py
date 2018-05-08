# -*- coding: utf-8 -*-
#author: 半熟的韭菜

from websocket import create_connection
import gzip
import time
import os,os.path
import datetime
import sys

NUMBER = 1
MAX_SIZE = 50000

def increment():
    global NUMBER
    NUMBER = NUMBER+1
    
def setNumber():
    DIR = 'ethusdt'
    global NUMBER
    NUMBER = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
    NUMBER = NUMBER+1

        
def addBracket(filename):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write('[' + content+']')

def getPath():
    #max_file_size=50000
    now = str(datetime.datetime.now())
    symbol = 'ethusdt'
    kind = 'market'
    mkdir(symbol)
    name = str(now)[:10]
    if(NUMBER < 10):
        path = symbol+'/'+name+'_'+symbol+'_'+kind+'_0'+str(NUMBER)+'.txt'
    else:
        path = symbol+'/'+name+'_'+symbol+'_'+kind+'_'+str(NUMBER)+'.txt'
    return path

def writeToJson(content):
    path = getPath() 
    # Write JSON file
    if(os.path.exists(path)):
        if(os.stat(path).st_size <= MAX_SIZE):
            with open(path,'a') as f:
                print(content)
                f.write(","+content)
        else:
            addBracket(path)
            increment()
            writeToJson(content)
            
    else:
        with open(path,'a') as f:
            print(content)
            f.write(content)
    
    



def mkdir(path):
    '''
    防止目录存在
    '''
    if not os.path.exists(path):
        os.mkdir(path)
        
        
def main():
    setNumber()
    while(1):
        try:
            ws = create_connection("wss://api.huobipro.com/ws")
            break
        except:
            print('connect ws error,retry...')
            time.sleep(5)
    
    # 订阅 KLine 数据
    #tradeStr="""{"sub": "market.ethusdt.kline.1min","id": "id10"}"""
    tradeStr="""{"req": "market.ethusdt.depth.step0", "id": "id10"}"""
    
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
    
    while(1):
        compressData=ws.recv()
        result=gzip.decompress(compressData).decode('utf-8')
        if result[:7] == '{"ping"':
            ts=result[8:21]
            pong='{"pong":'+ts+'}'
            ws.send(pong)
            ws.send(tradeStr)
        else:
            writeToJson(result)        
        
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        addBracket(getPath())
        print ('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        
            

