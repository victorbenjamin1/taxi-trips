import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from queries import *
import os
import matplotlib.pyplot as plt

cwd = os.getcwd()

sc = SparkContext('local')

spark = SparkSession.builder \
    .master("local") \
    .appName("Taxi Trip") \
    .config("spark.some.config.option", "some-value") \
    .config("spark.executor.memory", "70g") \
    .config("spark.driver.memory", "50g") \
    .config("spark.memory.offHeap.size", "16g") \
    .getOrCreate()

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
print(f'R: {resultado} milhas')

print("\n#########################################\n")

print('\n2. Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?\n')
BiggerVendors = spark.sql(query_question2)
totalamount_vendors = BiggerVendors.toPandas()
biggervendors = totalamount_vendors.head(3)


# BarChart

valores = biggervendors['total_arrecadado']
empresas = biggervendors['name']
plt.bar(empresas, valores)
barlist = plt.bar(empresas, valores)
barlist[0].set_color('b')
barlist[1].set_color('c')
barlist[2].set_color('y')
plt.xticks(fontsize=15)
plt.yticks(fontsize=13, rotation=45)
plt.title("Três maiores vendors em total de dinheiro arrecadado", fontsize=21)
plt.ylabel("Total arrecadado (em dólares)", fontsize=14)
figure = plt.gcf()
figure.set_size_inches(15, 10)
plt.savefig('files/results/barchart-question2.jpg')
plt.clf()
print(biggervendors[['name', 'total_arrecadado']].to_string())
print(f'\nGráfico: {cwd}/files/results/barchart-question2.jpg')

print("\n#########################################\n")


print('\n3. Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro\n')
MoneyTrips = spark.sql(query_question3)
moneytrips = MoneyTrips.toPandas()
moneytrips = moneytrips.sort_values(by=['mes_corrida'])
moneytrips.to_csv('files/results/moneytrips-question3.csv', index=False)

# Histograma

meses = moneytrips['mes_corrida']
qtdes = moneytrips['qtde_corridas']
plt.bar(meses, qtdes)
plt.xticks(rotation=45, fontsize=11)
plt.yticks(fontsize=13)
plt.title("Corridas pagas em dinheiro em 2009, 2010, 2011 e 2012", fontsize=19)
plt.ylabel("Quantidade de corridas", fontsize=12)
figure = plt.gcf()
figure.set_size_inches(19, 9)
plt.savefig('files/results/histogram-question3.jpg')
plt.clf()

print(f'CSV com os dados do histograma: {cwd}/files/results/moneytrips-question3.csv\n'
      f'Histograma: {cwd}/files/results/histogram-question3.jpg')


print("\n#########################################\n")

print('\n4. Faça um gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de '
      '2012\n')

Tips = spark.sql(query_question4)
tips_amount2012 = Tips.toPandas()
tips_amount2012 = tips_amount2012.sort_values(by=['dia_corrida'])
dias = tips_amount2012['dia_corrida']
tips = tips_amount2012['gorjetas']

plt.plot(dias, tips,  linestyle='--')
plt.xticks(rotation=45, fontsize=11)
plt.yticks(fontsize=12)
plt.title("Gorjetas por dia de 01/10/2012 até 31/12/2012", fontsize=19)
plt.ylabel("Quantidade de gorjetas", fontsize=13)
figure = plt.gcf()
figure.set_size_inches(19, 9)
plt.savefig('files/results/series-question4.jpg')
plt.clf()

tips_amount2012.to_csv('files/results/tipsamout-question4.csv', index=False)
print(f'CSV com os dados da série temporal: {cwd}/files/results/tipsamout-question4.csv\n'
      f'Série temporal: {cwd}/files/results/series-question4.jpg')

print("\n#########################################\n")

print('\n5. Bonûs - Qual o tempo médio das corridas nos dias de sábado e domingo;\n')

TripsWeekend = spark.sql(query_question5)
trips_weekend = TripsWeekend.toPandas()
trips_weekend['inicio_corrida_Date'] = trips_weekend['inicio_corrida'].astype('datetime64')
del trips_weekend['inicio_corrida']
trips_weekend['fim_corrida_Date'] = trips_weekend['fim_corrida'].astype('datetime64')
del trips_weekend['fim_corrida']

trips_weekend['elapsedTime'] = trips_weekend['fim_corrida_Date'] - trips_weekend['inicio_corrida_Date']

trips_weekend.to_csv('files/results/trips_weekend-question5.csv', index=False)
mean_time = trips_weekend['elapsedTime'].mean()


# formatando retorno tempo

def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


mean_time_format = strfdelta(mean_time, "{minutes} minutos e {seconds} segundos")

print(f'O tempo médio das corridas no final de semana é de {mean_time_format}\n'
      f'CSV com o tempo médio de cada corrida: {cwd}/files/results/trips_weekend-question5.csv \n')

print('\nDone!')
