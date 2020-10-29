#
""" 1. Qual a distância média percorrida por viagens com no máximo 2 passageiros?

        Nessa query foi obtida a média do campo trip_distance que considero ser
        a distância percorrida na viagem em milhas (considerando a unidade de medida
        mais utilizada nos EUA), filtrando apenas viagens que possuem o campo
        passenger_count (quantidade de passageiros) menor ou igual a 2. """

query_question1 = """
            SELECT  ROUND(AVG(trip_distance),2) avg_distance
            FROM trips
            WHERE passenger_count <= 2 """

# Explicacao

query_question2 = """
            SELECT  name, 
                    vendor_id,
                    SUM(total_amount) total_arrecadado
            FROM trips 
            LEFT JOIN vendors using(vendor_id) 
            GROUP by    name, 
                        vendor_id 
            ORDER BY total_arrecadado DESC """

# Explicacao

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

# Explicacao

query_question4 = """
            SELECT  DATE_FORMAT(pickup_datetime, 'y-MM-dd') dia_corrida, 
                    COUNT(1) gorjetas 
            from trips 
            WHERE   tip_amount > 0
                    AND DATE_FORMAT(pickup_datetime, 'y-MM-dd') BETWEEN '2012-10-01' AND '2012-12-31'
            GROUP BY dia_corrida """

# Explicacao

query_question5 = """
            SELECT    
                    DAYOFWEEK(DATE_FORMAT(pickup_datetime, 'y-MM-dd')) day_of_week, 
                    DATE_FORMAT(pickup_datetime, 'y-MM-dd H:mm:ss') inicio_corrida, 
                    DATE_FORMAT(dropoff_datetime, 'y-MM-dd H:mm:ss') fim_corrida
            FROM trips 
            WHERE DAYOFWEEK(DATE_FORMAT(pickup_datetime, 'y-MM-dd')) IN (1,7) """