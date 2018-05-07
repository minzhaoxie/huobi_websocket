# -*- coding: utf-8 -*-
#author: 半熟的韭菜

from websocket import create_connection
import gzip
import time
import os
import datetime
import sys
import os.path

jsonArray = []

COUNT = 0

def increment():
    global COUNT
    COUNT = COUNT+1



def writeToJson(content):
    #max_file_size=50000
    now = str(datetime.datetime.now())
    symbol = 'ethusdt'
    mkdir(symbol)
    name = str(now)[:10]
    path = symbol+'/'+name+'_'+symbol+'.txt'
    jsonArray.append(content)
    

    # Write JSON file
    if(os.path.exists(path)):
        if(COUNT==19):
            with open(path,'a') as f:
                print(content)
                f.write(","+content+']')
        else:
            with open(path,'a') as f:
                print(content)
                f.write(","+content)
    else:
        with open(path,'a') as f:
            print(content)
            f.write('['+content)
        
    increment()


def mkdir(path):
    '''
    防止目录存在
    '''
    if not os.path.exists(path):
        os.mkdir(path)
        
        
def main():
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
    
    while(COUNT<20):
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
        print ('Interrupted')
        sys.exit(0)
    
            

