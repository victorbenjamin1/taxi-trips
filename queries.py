
""" 1. Qual a distância média percorrida por viagens com no máximo 2 passageiros?

        Nessa query foi obtida a média do campo trip_distance que considero ser
        a distância percorrida na viagem em milhas (considerando a unidade de medida
        mais utilizada nos EUA), filtrando apenas viagens que possuam o campo
        passenger_count (quantidade de passageiros) menor ou igual a 2. """

query_question1 = """
            SELECT  ROUND(AVG(trip_distance),2) avg_distance
            FROM trips
            WHERE passenger_count <= 2 """


""" 2. Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?

        Foi obtida uma soma do campo total_amount para cada vendor.
        Ordenei os resultados pela soma obtida e com o LIMIT 3 peguei 
        só os três primeiros resultados.
        Fiz um JOIN com o dataset de vendors para capturar 
        o nome completo de cada empresa. """

query_question2 = """
            SELECT  name, 
                    vendor_id,
                    SUM(total_amount) total_arrecadado
            FROM trips 
            LEFT JOIN vendors using(vendor_id) 
            GROUP by    name, 
                        vendor_id 
            ORDER BY total_arrecadado DESC LIMIT 3"""


""" 3. Histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro;

        Foi obtida uma lista com a quantidade de corridas para cada mês.
        O campo de data utilizado foi pickup_datetime, a data de início da corrida.
        Fiz um JOIN com o dataset de payments-lookup para tratar possíveis
        inconsistências com o campo payment_type (tipo de pagamento).
        Depois de confiáveis os dados de payment_type, filtrei apenas
        pagamento em 'Cash' """

query_question3 = """
            SELECT  mes_corrida, 
                    count(1) qtde_corridas from (
                                SELECT 
                                    DATE_FORMAT(pickup_datetime, 'y-MM') mes_corrida, 
                                    payments.payment_lookup
                                FROM trips
                                LEFT JOIN payments ON
                                payments.payment_type = trips.payment_type
                                WHERE payment_lookup = 'Cash'
                                ) 
                    GROUP BY mes_corrida """


""" 4. Gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de 2012;

        Foi obtida uma lista com a quantidade de gorjetas para cada dia
        nos últimos 3 meses de 2012.
        Não existem dados de corrida posteriores ao dia 27/10/2012.
        O campo de data utilizado foi pickup_datetime, a data de início da corrida.
        Considerando que cada corrida tenha apenas uma gorjeta, fiz um contagem da quantidade
        de corridas que tem o tip_amount (valor total da gorjeta) maior que 0 """

query_question4 = """
            SELECT  DATE_FORMAT(pickup_datetime, 'y-MM-dd') dia_corrida, 
                    COUNT(1) gorjetas 
            from trips 
            WHERE   tip_amount > 0
                    AND DATE_FORMAT(pickup_datetime, 'y-MM-dd') BETWEEN '2012-10-01' AND '2012-12-31'
            GROUP BY dia_corrida """


""" 5. Qual o tempo médio das corridas nos dias de sábado e domingo;

        Foi obtida uma lista com a momento de inicio e fim de todas corridas feitas
        nos finais de semana.
        A função DAYOFWEEK retorna o dia da semana da data especifícada, isso serviu
        como filtro onde selecionei apenas o que retornou 1 (domingo) e 7 (sábado). 
        O restante da análise foi processada pelo pandas.
        Com o ínicio e fim de cada corrida definido, foi possível processar o tempo
        gasto em cada corrida para posteriormente tirar a média do todo """

query_question5 = """
            SELECT    
                    DAYOFWEEK(DATE_FORMAT(pickup_datetime, 'y-MM-dd')) day_of_week, 
                    DATE_FORMAT(pickup_datetime, 'y-MM-dd H:mm:ss') inicio_corrida, 
                    DATE_FORMAT(dropoff_datetime, 'y-MM-dd H:mm:ss') fim_corrida
            FROM trips 
            WHERE DAYOFWEEK(DATE_FORMAT(pickup_datetime, 'y-MM-dd')) IN (1,7) """