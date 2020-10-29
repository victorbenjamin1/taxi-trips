import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from queries import *
import matplotlib.pyplot as plt

sc = SparkContext('local')
spark = SparkSession(sc)

# Carregando Dataset: Taxi Trips

print('\nCarregando Dataset: Taxi Trips')

path = "files/datasets/data-sample_data-n*"
Trips = spark.read.json(path)
Trips.createOrReplaceTempView("trips")

# Carregando Dataset: Vendors

print('Carregando Dataset: Vendors')

vendors_path = 'files/datasets/data-vendor_lookup.csv'
Vendors = spark.read.option("header", True).csv(vendors_path)
Vendors.createOrReplaceTempView("vendors")

# Carregando Dataset: Payments Lookup

print('Carregando Dataset: Payments Lookup')

paymn = pd.read_csv('files/datasets/data-payment_lookup.csv', skiprows=[0])
paymn.to_csv('files/datasets/data_payments.csv')
payments_path = 'files/datasets/data_payments.csv'
Payments = spark.read.option("header", True).csv(payments_path)
Payments.createOrReplaceTempView("payments")

# Início da resolução das questões

print('\n1. Qual a distância média percorrida por viagens com no máximo 2 passageiros?')
DistanciaMedia = spark.sql(query_question1)
distancia_media = DistanciaMedia.toPandas()
resultado = distancia_media['avg_distance'][0]
print(f'R: A distância média percorrida com viagens de no máximo 2 dos passageiros é de {resultado} milhas')

print("\n#########################################\n")

print('\n2. Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?\n')
BiggerVendors = spark.sql(query_question2)
totalamount_vendors = BiggerVendors.toPandas()
biggervendors = totalamount_vendors.head(3)
print(biggervendors[['name', 'total_arrecadado']].to_string())

print("\n#########################################\n")

print('\n3. Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro\n')
MoneyTrips = spark.sql(query_question3)
moneytrips = MoneyTrips.toPandas()
moneytrips.to_csv('files/results/moneytrips-question3.csv', index=False)
print(f'CSV com os dados do histograma: /link/ \n'
      f'Histograma: /link/')

print("\n#########################################\n")

print('\n4. Faça um gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de '
      '2012\n')

Tips = spark.sql(query_question4)
tips_amount2012 = Tips.toPandas()
tips_amount2012 = tips_amount2012.sort_values(by=['dia_corrida'])
tips_amount2012.to_csv('files/results/tipsamout-question4.csv', index=False)
print(f'CSV com os dados da série temporal: /link/ \n'
      f'Série temporal: /link/')

print("\n#########################################\n")

print('\n5. Bonûs - Qual o tempo médio das corridas nos dias de sábado e domingo;\n')

TripsWeekend = spark.sql(query_question5)
trips_weekend = TripsWeekend.toPandas()
trips_weekend['inicio_corrida_Date'] = trips_weekend['inicio_corrida'].astype('datetime64')
del trips_weekend['inicio_corrida']
trips_weekend['fim_corrida_Date'] = trips_weekend['fim_corrida'].astype('datetime64')
del trips_weekend['fim_corrida']

elaptime = []

for i, row in trips_weekend.iterrows():
    elapsedTime = row['fim_corrida_Date'] - row['inicio_corrida_Date']
    elapsedTime = elapsedTime.total_seconds()
    elaptime.append(elapsedTime)

trips_weekend['elapsedTime'] = elaptime
trips_weekend.to_csv('files/results/trips_weekend-question5.csv', index=False)
mean_time = trips_weekend['elapsedTime'].mean()
print(f'O tempo médio das corridas no final de semana é de: {round(mean_time)} segundos\n'
      f'CSV o tempo médio de cada corrida: /link/ \n')
