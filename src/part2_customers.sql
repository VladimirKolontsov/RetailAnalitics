-- Active: 1711450086209@@127.0.0.1@5432@postgres

----------------------------- Create table: segments for Customer_Segment-----------------------------------------

create table if not exists segments (
    segment BIGINT PRIMARY KEY,
    average_check CHARACTER(20),
    frequency_of_purchases CHARACTER(20),
    churn_probability CHARACTER(20)
);

call import_data_from_tsv (
    'segments',
    '/Users/vladimirkoloncov/IdeaProjects/retail_analitics_21/src/tsv/Segments.tsv',
    E'\t'
);

----------------------------- Create table: segments for Customer_Segment-----------------------------------------

DROP MATERIALIZED VIEW IF EXISTS Customers_View CASCADE;
--------------------------- Drop materialized view: Customers_View ---------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS Customers_View AS
WITH
    basic_info AS (
        SELECT
            customer_id,
            (avg(transaction_summ)) AS customer_average_check,
            (
                EXTRACT(
                    EPOCH
                    FROM (
                            MAX(transaction_date_time) - MIN(transaction_date_time)
                        )
                ) / 86400 / COUNT(transaction_id)
            ) AS customer_frequency,
            (
                EXTRACT(
                    EPOCH
                    FROM (
                            (
                                SELECT MAX(analysis_formation)
                                FROM date_of_analysis_formation
                            ) - MAX(transaction_date_time)
                        )
                ) / 86400
            ) AS customer_inactive_period -- convert to days
        FROM customer_cards
            JOIN transactions USING (customer_card_id)
        GROUP BY
            customer_id
        ORDER BY customer_id
    ),
    add_cume_dist AS (
        SELECT
            customer_id,
            PERCENT_RANK() OVER (
                ORDER BY customer_average_check DESC
            ) AS cume_check,
            PERCENT_RANK() OVER (
                ORDER BY customer_frequency
            ) AS cume_frequancy,
            customer_inactive_period / customer_frequency AS customer_churn_rate
        FROM basic_info
        ORDER BY customer_id
    ),
    segment_data AS (
        SELECT
            customer_id,
            customer_churn_rate,
            CASE
                WHEN cume_check <= 0.1 THEN 'High'
                WHEN cume_check > 0.1
                AND cume_check <= 0.25 THEN 'Medium'
                ELSE 'Low'
            END AS customer_average_check_segment,
            CASE
                WHEN cume_frequancy <= 0.1 THEN 'Often'
                WHEN cume_frequancy > 0.1
                AND cume_frequancy <= 0.35 THEN 'Occasionally'
                ELSE 'Rarely'
            END AS customer_frequency_segment,
            CASE
                WHEN customer_churn_rate > 5 THEN 'High'
                WHEN customer_churn_rate > 2
                AND customer_churn_rate <= 5 THEN 'Medium'
                ELSE 'Low'
            END AS customer_churn_segment
        FROM add_cume_dist
    ),
    segment_num AS (
        SELECT customer_id, s.segment AS customer_segment
        FROM
            segment_data d
            JOIN segments s ON d.customer_average_check_segment = s.average_check
            AND d.customer_frequency_segment = s.frequency_of_purchases
            AND d.customer_churn_segment = s.churn_probability
    ),
    data_for_visits AS (
        SELECT
            customer_id,
            customer_card_id,
            transaction_store_id,
            transaction_id,
            transaction_date_time
        FROM customer_cards
            JOIN transactions USING (customer_card_id)
    ),
    -- Определение перечня магазинов клиента
    customer_stores AS (
        SELECT
            customer_id,
            transaction_store_id,
            last_visit,
            COUNT(*)::numeric / totat_transaction AS transactions_part
        FROM (
                SELECT
                    t.customer_id, t.transaction_store_id, MAX(t.transaction_date_time) OVER (
                        PARTITION BY
                            t.customer_id, t.transaction_store_id
                    ) AS last_visit, SUM(COUNT(t.transaction_id)) OVER (
                        PARTITION BY
                            t.customer_id
                    ) AS totat_transaction
                FROM data_for_visits t
                WHERE
                    t.transaction_date_time <= (
                        SELECT MAX(analysis_formation)
                        FROM date_of_analysis_formation
                    )
                GROUP BY
                    t.customer_id, t.transaction_store_id, t.transaction_date_time
            ) s
        GROUP BY
            customer_id,
            transaction_store_id,
            last_visit,
            totat_transaction
        ORDER BY
            customer_id,
            transactions_part DESC,
            last_visit DESC
    ),
    top_tree AS (
        SELECT *
        FROM (
                SELECT t.customer_id, t.transaction_store_id, t.transaction_date_time, ROW_NUMBER() OVER (
                        PARTITION BY
                            t.customer_id
                        ORDER BY t.transaction_date_time DESC
                    ) AS num
                FROM data_for_visits t
                WHERE
                    t.transaction_date_time <= (
                        SELECT MAX(analysis_formation)
                        FROM date_of_analysis_formation
                    )
            ) s
        WHERE
            num <= 3
    ),
    top_store AS (
        SELECT
            tt.customer_id,
            CASE
                WHEN COUNT(
                    DISTINCT tt.transaction_store_id
                ) = 1 THEN MAX(tt.transaction_store_id)
                ELSE (
                    SELECT cs.transaction_store_id
                    FROM customer_stores cs
                    WHERE
                        cs.customer_id = tt.customer_id
                    LIMIT 1
                )
            END AS customer_primary_store
        FROM top_tree tt
        GROUP BY
            tt.customer_id
    )
SELECT c.customer_id, b.customer_average_check, s.customer_average_check_segment, b.customer_frequency, s.customer_frequency_segment, b.customer_inactive_period, s.customer_churn_rate, s.customer_churn_segment, t.customer_primary_store
FROM
    segment_data s
    JOIN basic_info b USING (customer_id)
    JOIN segment_num n USING (customer_id)
    JOIN top_store t USING (customer_id)
    RIGHT JOIN customers c USING (customer_id);

--------------------------- Materialized view: Customers_View --------------------------------

SELECT *
FROM Customers_View;
    --------------------------- Test -----------------------------