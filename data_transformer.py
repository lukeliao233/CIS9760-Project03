import boto3
import os
import sys
import json 
from time import sleep
import yfinance as yf

kinesis = boto3.client(os.environ["boto"], os.environ["region"])
def lambda_handler(event, context):
    companies = ["AMZN", "BABA", "WMT", "EBAY", "SHOP", "TGT", "BBY", "HD", "COST", "KR"]
    for company in companies:
        company_ticker = yf.Ticker(company)
        hist = company_ticker.history(start="2022-10-24", end="2022-11-05", interval = "5m")
        for index, row in hist.iterrows():
            info = {"high":round(row["High"], 2), "low":round(row["Low"], 2), "volatility":round(row["High"] - row["Low"], 2), "ts":index.strftime('%Y-%m-%d %X'), "name":company}
            as_jsonstr = json.dumps(info)+"\n"
            output = kinesis.put_record(
                StreamName = os.environ["streamname"],
                Data=as_jsonstr,
                PartitionKey="partitionkey")
            sleep(0.08)
            print(as_jsonstr)
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')}    