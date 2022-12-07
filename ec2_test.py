import json
import boto3
import pandas as pd

pricing_client = boto3.client('pricing', region_name='us-east-1')

def get_products(region):
    paginator = pricing_client.get_paginator('get_products')
   # print('cristian1-->',paginator)
    response_iterator = paginator.paginate(
        ServiceCode="AmazonEC2",
        Filters=[
            {
                'Type': 'TERM_MATCH',
                'Field': 'location',
                'Value': region
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'instanceType',
                'Value': 't2.nano'
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'operatingSystem',
                'Value': 'Linux'
            },
            {
                'Type': 'TERM_MATCH',
                'Field': "marketoption",
                'Value': "OnDemand"
            },
        ],
        PaginationConfig={
            'PageSize': 100
        }
    )
    #print('cristian2-->',response_iterator)
    products = []
    num = 0
    for response in response_iterator:
        print('api con header-->', response)
        for priceItem in response["PriceList"]:
            print('api capturado-->', priceItem)
            num =+ num + 1
            print('----------------------',num)
            priceItemJson = json.loads(priceItem)
            df = pd.DataFrame(priceItemJson)
            
            excel = df.to_csv(f"excel{num}.csv")
            print('-----------------------------',df,'este es el df------------------------------------')
            
            products.append(priceItemJson)

    print('productos no se que onda----------------------------------------------------------------------------------------primero',products,'productos no se que onda----------------------------------------------------------------------------------------')
    cantProd = 0
    for x in products:
        cantProd =+ cantProd + 1
        df2 = pd.DataFrame(x)
        excel2 = df.to_csv(f'productos{cantProd}.csv')

if __name__ == '__main__':
    get_products('US East (N. Virginia)')