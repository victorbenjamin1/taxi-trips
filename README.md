# taxi-trips

## O que é?

Esse projeto tem por objetivo realizar análises sobre um conjunto de dados a respeito de corridas de táxi realizadas em nova york entre os anos de 2009 e 2012, além de fornecer a infraestrutura necessária em cloud para reprodução do processamento.

## Tecnologias utilizadas

- AWS EC2 (Linux)
- AWS S3
- AWS CLI
- Python (pandas, pyspark, matplotlib) 
- SQL


## Tópicos que serão respondidos na análise:

1. Qual a distância média percorrida por viagens com no máximo 2 passageiros?
2. Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?
3. Histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro.
4. Gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de 2012.
5. Qual o tempo médio das corridas nos dias de sábado e domingo?


## Arquivo de querys

No arquivo **queries.py** disponível na raiz do projeto estão todas as queries utilizadas em cada questão, acompanhadas de uma breve descrição da linha de pensamento considerada.

## Para reproduzir as análises a partir de um ambiente pronto em nuvem:

1. Faça o download do arquivo com extensão **.pem** disponibilizado pelo dono do projeto. [AJUSTAR] Mandar zipado com senha

2. Garanta que a chave tenha a permissão adequada:

`chmod 400 ravya-ec2instance.pem`

3. Para conectar na máquina disponbilizada execute:

`ssh -i ravya-ec2instance.pem ubuntu@3.90.8.182`

4. Vá até o diretório onde se encontra o projeto:

`cd /home/ubuntu/projects/taxi-trips`

5. Execute o comando abaixo para sincronizar o diretório local com bucket do s3.
Esse comando fará o download de todo o dataset.

`python3 download-files.py`

   **OBS:** *Se todos os arquivos já estiverem sincronizados nada vai acontecer.*
   
5. Agora, para iniciar o processamento basta executar o comando abaixo:

`python3 main.py`

Nesse momento os dados são processados e os resultados são imediatamente são apresentados no terminal em que o programa foi executado.

