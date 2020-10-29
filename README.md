# taxi-trips

## O que é?

## Tecnologias utilizadas

## Perguntas que precisam ser respondidas

## Arquivo de querys

## Para reproduzir as análises a partir do ambiente em nuvem:

1. Faça o download do arquivo com extensão **.pem** disponível na raiz do projeto.

2. Garanta que ela tenha a permissão adequada:

`chmod 400 ravya-ec2instance.pem`

3. Para conectar na máquina execute:

`ssh -i ravya-ec2instance.pem ubuntu@3.90.8.182`

4. Vá até o diretório onde se encontra o projeto:

`cd //`

5. Execute o comando abaixo para sincronizar o diretório local com bucket do s3

`python download-files.py`

> OBS: Se todos os arquivos já estiverem sincronizados nada vai acontecer.
