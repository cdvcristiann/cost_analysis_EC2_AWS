import json
import boto3
import pandas as pd
import pickle

client = boto3.client('pricing', region_name='us-east-1')

response = client.get_products(
    ServiceCode='AmazonEC2',
    Filters=[
        {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
        {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': 'US East (N. Virginia)'},
        {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': 't2.small'},
        {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
        {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'}
    ]
)
cantProd = 0
#print(response)


for pricelist_json in response["PriceList"]:
    cantProd +=1
    pricelist = json.loads(pricelist_json)
    dataframe = pd.DataFrame(pricelist)
    jsonArch = dataframe.to_json(f'price_t2_small{cantProd}.json')


    with open(f'datos{cantProd}.json', 'w') as fp:
        json.dump(pricelist, fp, ensure_ascii=False)

   

#    product = pricelist['product']
#    if product['productFamily'] == 'Compute Instance':
#        listaP = list(pricelist['terms']['OnDemand'].values())[0]
 #       listaP2 = list(pricelist['terms'].keys())
  #      precioUSD = list()
 #       for x in listaP2:
  #          print('***********',x)
   #         if x == 'Reserved':
    #            listaP3 = list(pricelist['terms'][x].values())[0]
#                print('++++++++dentro del if p3++++++++',listaP3)
 #               listaP4 = list(listaP3['priceDimensions'].values())
  #              df = pd.DataFrame(listaP4)
   #             print(df,'data frame p4')
    #            productos = df.to_csv(f'productos{cantProd}.csv')

#                df2 = pd.DataFrame(listaP3)
 #               print(df2,'data frame p3')
  #              excel = df2.to_csv(f'excel{cantProd}.csv')
   #             cantProd +=cantProd +1
    #    print(listaP)
       # print(f"{list(listaP[0]['priceDimensions'].values())[0]}")
      #  print(f"{list(listaP[0]['termAttributes'].values())}")
