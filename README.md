# taxi-trips

## O que é?

## Tecnologias utilizadas

## Tópicos que respondidos com essa análise:

1. Qual a distância média percorrida por viagens com no máximo 2 passageiros?
2. Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?
3. Histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro.
4. Gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de 2012.
5. Qual o tempo médio das corridas nos dias de sábado e domingo?


## Arquivo de querys

No arquivo **queries.py** disponível na raiz do projeto todas as queries utilizadas em cada questão, acompanhadas de uma breve descrição da linha de pensamento considerada.

## Para reproduzir as análises a partir do ambiente em nuvem:

1. Faça o download do arquivo com extensão **.pem** disponível na raiz do projeto.

2. Garanta que ela tenha a permissão adequada:

`chmod 400 ravya-ec2instance.pem`

3. Para conectar na máquina execute:

`ssh -i ravya-ec2instance.pem ubuntu@3.90.8.182`

4. Vá até o diretório onde se encontra o projeto:

`cd /home/ubuntu/projects/taxi-trips`

5. Execute o comando abaixo para sincronizar o diretório local com bucket do s3

`python download-files.py`

   **OBS:** *Se todos os arquivos já estiverem sincronizados nada vai acontecer.*
