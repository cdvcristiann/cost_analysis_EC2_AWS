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


df_json1 = pd.read_json('price_t2_small1.json',orient='index')
df_json2 = pd.read_json('price_t2_small2.json',orient='index')
df_json3 = pd.read_json('price_t2_small3.json',orient='index')
df = pd.concat([df_json1,df_json2,df_json3])


onDemand = df['OnDemand']

reserved = df['Reserved']
termsReserved = reserved['terms']
termsReserved.dropna(inplace=True)

cant = 0
datas = {}

for x in termsReserved:
  for y in x.values():
    priceDimensions=y['priceDimensions']
    termAttributes=y['termAttributes']
    LeaseContractLength=y['termAttributes']['LeaseContractLength']
    for y in priceDimensions.values():
      df = pd.Series(y)
      pricePerUnit = y['pricePerUnit']['USD']
      pricePerUnitS = f'{pricePerUnit}'
      pricePerUnitIF = y['pricePerUnit']
      description = y['description']
      df[df == pricePerUnitIF] = pricePerUnitS
      for j in termAttributes.values():
        df2 = pd.Series(j)
        cant =+cant + 1
        df3 = pd.concat([df,df2])
        datas[f'data{cant}'] = df3
        sr = pd.Series(termAttributes)


df_general = pd.DataFrame(datas)


datos1 = {}
datos2 = {}
datos3 = {}
datos4 = {}
datos5 = {}
datos6 = {}
datos7 = {}
datos8 = {}
datos9 = {}
datos10 = {}
datos11 = {}
datos12 = {}
datos13 = {}
datos14 = {}
datos15 = {}
datos16 = {}
datos17 = {}

precios=[]
cant_data =0

for precio in df_general:
  indice= df_general[f'{precio}'].pricePerUnit
  precios.append(indice)

lista_precios = pd.unique(precios)

for x in df_general:
  cant_data+=1
  if df_general[f'{x}'].pricePerUnit == lista_precios[0]:
    datos1[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[1]:
    datos2[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[2]:
    datos3[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[3]:
    datos4[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[4]:
    datos5[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[5]:
    datos6[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[6]:
    datos7[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[7]:
    datos8[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[8]:
    datos9[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[9]:
    datos10[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[10]:
    datos11[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[11]:
    datos12[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[12]:
    datos13[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[13]:
    datos14[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[14]:
    datos15[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[15]:
    datos16[f'data{cant_data}'] = df_general[f'{x}']
  if df_general[f'{x}'].pricePerUnit == lista_precios[16]:
    datos17[f'data{cant_data}'] = df_general[f'{x}']
   


def borrar_datos_basuras(datos,lista_Salida):
  cantidad=0  
  for x in datos.keys():
    cantidad+=1
    datos[x].pop('appliesTo')
    datos[x].pop('rateCode')
    datos[x].pop('endRange')
    datos[x].pop('beginRange')
    lista_Salida.append(datos[x])


lista_datos1 = []
borrar_datos_basuras(datos1,lista_datos1)
lista_datos2 = []
borrar_datos_basuras(datos2,lista_datos2)
lista_datos3 = []
borrar_datos_basuras(datos3,lista_datos3)
lista_datos4 = []
borrar_datos_basuras(datos4,lista_datos4)
lista_datos5 = []
borrar_datos_basuras(datos5,lista_datos5)
lista_datos6 = []
borrar_datos_basuras(datos6,lista_datos6)
lista_datos7 = []
borrar_datos_basuras(datos7,lista_datos7)
lista_datos8 = []
borrar_datos_basuras(datos8,lista_datos8)
lista_datos9 = []
borrar_datos_basuras(datos9,lista_datos9)
lista_datos10 = []
borrar_datos_basuras(datos10,lista_datos10)
lista_datos11 = []
borrar_datos_basuras(datos11,lista_datos11)
lista_datos12 = []
borrar_datos_basuras(datos12,lista_datos12)
lista_datos13 = []
borrar_datos_basuras(datos13,lista_datos13)
lista_datos14 = []
borrar_datos_basuras(datos14,lista_datos14)
lista_datos15 = []
borrar_datos_basuras(datos15,lista_datos15)
lista_datos16 = []
borrar_datos_basuras(datos16,lista_datos16)
lista_datos17 = []
borrar_datos_basuras(datos17,lista_datos17)



df1 = pd.DataFrame(lista_datos1)
df2 = pd.DataFrame(lista_datos2)
df3 = pd.DataFrame(lista_datos3)
df4 = pd.DataFrame(lista_datos4)
df5 = pd.DataFrame(lista_datos5)
df6 = pd.DataFrame(lista_datos6)
df7 = pd.DataFrame(lista_datos7)
df8 = pd.DataFrame(lista_datos8)
df9 = pd.DataFrame(lista_datos9)
df10 = pd.DataFrame(lista_datos10)
df11 = pd.DataFrame(lista_datos11)
df12 = pd.DataFrame(lista_datos12)
df13 = pd.DataFrame(lista_datos13)
df14 = pd.DataFrame(lista_datos14)
df15 = pd.DataFrame(lista_datos15)
df16 = pd.DataFrame(lista_datos16)
df17 = pd.DataFrame(lista_datos17)


df1 = df1.drop_duplicates()
df2 = df2.drop_duplicates()
df3 = df3.drop_duplicates()
df4 = df4.drop_duplicates()
df5 = df5.drop_duplicates()
df6 = df6.drop_duplicates()
df7 = df7.drop_duplicates()
df8 = df8.drop_duplicates()
df9 = df9.drop_duplicates()
df10 = df10.drop_duplicates()
df11 = df11.drop_duplicates()
df12 = df12.drop_duplicates()
df13 = df13.drop_duplicates()
df14 = df14.drop_duplicates()
df15 = df15.drop_duplicates()
df16 = df16.drop_duplicates()
df17 = df17.drop_duplicates()

def datos_obtenidos(datosparafor,datosparaextraer,nombre_Salida,):
  LeaseContractLength = {}
  OfferingClass = {}
  PurchaseOption = {}
  descripcion={}
  for x in datosparafor:
    if 'yr' in x:
      LeaseContractLength['data1']=x
    if ('convertible' in x) or 'standar' in x:
      OfferingClass['data1']=x
    if ('No Upfront' in x) or ('Partial Upfront' in x) or ('All Upfront' in x):
      PurchaseOption['data1']=x
    

  LeaseContractLength
  LeaseContractLength=LeaseContractLength['data1']

  OfferingClass = OfferingClass['data1']
  descripcion = datosparaextraer['description']
  pricePerUnit = datosparaextraer['pricePerUnit']
  unit = datosparaextraer['unit']
  PurchaseOption = PurchaseOption['data1']
  nombre_Salida = {'description':descripcion,'pricePerUnit':pricePerUnit,'unit':unit,'LeaseContractLength':LeaseContractLength,'OfferingClass':OfferingClass,'PurchaseOption':PurchaseOption}
  return nombre_Salida



def eliminar_duplicados(dataf):
  nombre_Salida = {}
  datosparafor = dataf[0]
  datosparaextraer=dataf
  datos19 = datos_obtenidos(datosparafor,datosparaextraer,nombre_Salida)
  df = pd.DataFrame(datos19)
  df.drop_duplicates(inplace=True)
  return df


df1 = eliminar_duplicados(df1)
df2 = eliminar_duplicados(df2)
df3 = eliminar_duplicados(df3)
df4 = eliminar_duplicados(df4)
df5 = eliminar_duplicados(df5)
df6 = eliminar_duplicados(df6)
df7 = eliminar_duplicados(df7)
df8 = eliminar_duplicados(df8)
df9 = eliminar_duplicados(df9)
df10 = eliminar_duplicados(df10)
df11 = eliminar_duplicados(df11)
df12 = eliminar_duplicados(df12)
df13 = eliminar_duplicados(df13)
df14 = eliminar_duplicados(df14)
df15 = eliminar_duplicados(df15)
df16 = eliminar_duplicados(df16)
df17 = eliminar_duplicados(df17)


data10 = pd.concat([df1,df2,df3,df4,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17])
data10 = pd.DataFrame(data10)

onDemand = onDemand[onDemand.notna()]
onDemand = onDemand['terms']

cant = 0
datasOndemand = {}

for x in onDemand:
  for y in x.values():
    print(x)
    priceDimensions=y['priceDimensions']
    termAttributes=y['termAttributes']
    for z in priceDimensions.values():
      df = pd.Series(z)
      pricePerUnit = z['pricePerUnit']['USD']
      pricePerUnitS = f'{pricePerUnit}'
      pricePerUnitIF = z['pricePerUnit']
      description = z['description']
      df[df == pricePerUnitIF] = pricePerUnitS
      cant+=1
      datasOndemand[f'data{cant}'] = df


df_general = pd.DataFrame(datasOndemand)



def borrar_datos_basuras(datos,lista_Salida):
  cantidad=0  
  for x in datos:
    cantidad+=1 
    datos[x].pop('appliesTo')
    datos[x].pop('rateCode')
    datos[x].pop('beginRange')
    datos[x].pop('endRange')
    lista_Salida.append(datos[x])


lista = []
borrar_datos_basuras(df_general,lista)

df1 = pd.DataFrame(lista)



df1.drop_duplicates(inplace=True)

union = pd.concat([df1,data10])

indices = []
for i in range(union.shape[0]):
  indices.append(0)

union.index = indices


union['OfferingClass'].fillna('On Demand', inplace=True)


union.to_csv(f'apiDatoslimpios.csv')
union.sort_values(by = 'LeaseContractLength')

seleccion_allUfront_3yr = (union['PurchaseOption'] == 'All Upfront') & (union['LeaseContractLength'] == '3yr') & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
allUfront_3yr = union[seleccion_allUfront_3yr]


cost_analysis = {}

seleccion_partial_upfront_3yr_Quantity = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '3yr') & (union['unit']== 'Quantity')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
partial_upfront_3yr_Quantity = union[seleccion_partial_upfront_3yr_Quantity]

seleccion_partial_upfront_3yr_Hrs = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '3yr') & (union['unit']== 'Hrs')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
partial_upfront_3yr_Hrs = union[seleccion_partial_upfront_3yr_Hrs]


seleccion_no_upfront_3yr = (union['PurchaseOption'] == 'No Upfront') & (union['LeaseContractLength'] == '3yr')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
no_upfront_3yr = union[seleccion_no_upfront_3yr]


seleccion_allUfront_1yr = (union['PurchaseOption'] == 'All Upfront') & (union['LeaseContractLength'] == '1yr')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
allUfront_1yr = union[seleccion_allUfront_1yr]



seleccion_partial_upfront_1yr_Quantity_standard = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '1yr') & (union['unit']== 'Quantity')& (union['OfferingClass']== 'standard')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
partial_upfront_1yr_Quantity_standard = union[seleccion_partial_upfront_1yr_Quantity_standard]


seleccion_partial_upfront_1yr_Quantity_convertible = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '1yr') & (union['unit']== 'Quantity')& (union['OfferingClass']== 'convertible')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
partial_upfront_1yr_Quantity_convertible = union[seleccion_partial_upfront_1yr_Quantity_convertible]


seleccion_partial_upfront_1yr_Hrs_standard = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '1yr') & (union['unit']== 'Hrs')& (union['OfferingClass']== 'standard')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
partial_upfront_1yr_Hrs_standard = union[seleccion_partial_upfront_1yr_Hrs_standard]


seleccion_partial_upfront_1yr_Hrs_convertible = (union['PurchaseOption'] == 'Partial Upfront') & (union['LeaseContractLength'] == '1yr') & (union['unit']== 'Hrs')& (union['OfferingClass']== 'convertible')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000 
partial_upfront_1yr_Hrs_convertible = union[seleccion_partial_upfront_1yr_Hrs_convertible]



seleccion_partial_upfront_1yr_Quantity = pd.concat([partial_upfront_1yr_Quantity_standard,partial_upfront_1yr_Quantity_convertible])

seleccion_partial_upfront_1yr_Hrs = pd.concat([partial_upfront_1yr_Hrs_standard,partial_upfront_1yr_Hrs_convertible])

seleccion_no_upfront_1yr_convertible = (union['PurchaseOption'] == 'No Upfront') & (union['LeaseContractLength'] == '1yr') & (union['OfferingClass']== 'convertible')  & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
no_upfront_1yr_convertible = union[seleccion_no_upfront_1yr_convertible]


seleccion_no_upfront_1yr_standard = (union['PurchaseOption'] == 'No Upfront') & (union['LeaseContractLength'] == '1yr') & (union['OfferingClass']== 'standard') & pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000
no_upfront_1yr_standard = union[seleccion_no_upfront_1yr_standard]

no_upfront_1yr = pd.concat([no_upfront_1yr_standard,no_upfront_1yr_convertible])

seleccion_on_demand = (union['OfferingClass'] == 'On Demand') & (pd.to_numeric(union['pricePerUnit'], errors = 'coerce') > 0.000)

on_demand = union[seleccion_on_demand]
on_demand

cost_analysis['OfferingClass'] = allUfront_1yr['OfferingClass']
cost_analysis['Reserved-3 Year All Upfront'] = allUfront_3yr['pricePerUnit']
cost_analysis['Reserved-3 Year Partial Upfront'] = ((pd.to_numeric(partial_upfront_3yr_Hrs['pricePerUnit'], errors = 'coerce') * 730) * int(36)) + (pd.to_numeric(partial_upfront_3yr_Quantity['pricePerUnit'], errors = 'coerce'))
cost_analysis['Reserved-3 Year No Upfront'] =((pd.to_numeric(no_upfront_3yr['pricePerUnit'], errors = 'coerce') * 730) * int(36))
cost_analysis['Reserved-1 Year All Upfront'] = allUfront_1yr['pricePerUnit']
cost_analysis['Reserved-1 Year Partial Upfront'] = ((pd.to_numeric(seleccion_partial_upfront_1yr_Hrs['pricePerUnit'], errors = 'coerce') * 730) * int(12)) + (pd.to_numeric(seleccion_partial_upfront_1yr_Quantity['pricePerUnit'], errors = 'coerce'))
cost_analysis['Reserved-1 Year No Upfront'] =((pd.to_numeric(no_upfront_1yr['pricePerUnit'], errors = 'coerce') * 730) * int(12))
cost_analysis['On-Demand/Mes'] =(pd.to_numeric(on_demand['pricePerUnit'], errors = 'coerce') * 730)
cost_analysis['On-Demand/1year'] =(cost_analysis['On-Demand/Mes'] * 12)
cost_analysis 



dict_offerinClass = {}
for i, j in cost_analysis.items():
  dict_offerinClass[i] = j



for i in dict_offerinClass:
  dict_offerinClass[i].index = range(dict_offerinClass[i].shape[0])



dict_offerinClass = pd.DataFrame(dict_offerinClass)
dict_offerinClass.to_csv('data_analysis_con_null.csv')

dict_offerinClass.fillna(method='ffill', inplace=True)
dict_offerinClass.to_csv('data_analysis_sin_null.csv')