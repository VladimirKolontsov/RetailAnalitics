DROP MATERIALIZED VIEW IF EXISTS Groups_View CASCADE;
--------------------------- Drop materialized view: Groups_View ---------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS Groups_View AS
WITH
    base_info AS (
        SELECT DISTINCT
            c.customer_id,
            pm.group_id
        FROM
            checks ch
            JOIN transactions t USING (transaction_id)
            JOIN customer_cards cc USING (customer_card_id)
            JOIN product_matrix pm USING (sku_id)
            JOIN customers c USING (customer_id)
        WHERE
            transaction_date_time <= (
                SELECT MAX(analysis_formation)
                FROM date_of_analysis_formation
            )
        ORDER BY c.customer_id, pm.group_id
    ),
    affinity_index AS (
        SELECT b.customer_id, b.group_id, (
                CAST(Group_Purchase AS FLOAT) / COUNT(DISTINCT transaction_id)
            ) AS Group_Affinity_Index
        FROM
            base_info b
            JOIN period_view p USING (customer_id, group_id)
            JOIN purchase_history_view phv USING (customer_id)
        WHERE
            transaction_date_time BETWEEN First_Group_Purchase_Date AND Last_Group_Purchase_Date
        GROUP BY
            b.customer_id,
            b.group_id,
            Group_Purchase
        ORDER BY customer_id
    ),
    churn_rate AS (
        SELECT
            customer_id,
            group_id,
            (
                (
                    EXTRACT(
                        epoch
                        FROM (
                                SELECT MAX(analysis_formation)
                                FROM date_of_analysis_formation
                            )
                    ) - EXTRACT(
                        epoch
                        FROM (MAX(transaction_date_time))
                    )
                ) / Group_Frequency / 86400
            ) AS Group_Churn_Rate,
            SUM(Group_Summ_Paid - Group_Cost) AS Group_Margin
        FROM
            purchase_history_view
            JOIN period_view USING (customer_id, group_id)
        GROUP BY
            customer_id,
            group_id,
            Group_Frequency
        ORDER BY customer_id, group_id
    ),
    consumption_stability AS (
        WITH
            intervals AS (
                SELECT
                    customer_id,
                    group_id,
                    transaction_date_time,
                    LAG(transaction_date_time) OVER (
                        PARTITION BY
                            customer_id,
                            group_id
                        ORDER BY transaction_date_time
                    ) AS previous_transaction_date_time,
                    EXTRACT(
                        EPOCH
                        FROM (
                                transaction_date_time - LAG(transaction_date_time) OVER (
                                    PARTITION BY
                                        customer_id, group_id
                                    ORDER BY transaction_date_time
                                )
                            )
                    ) / 86400 AS interval_days
                FROM purchase_history_view
            )
        SELECT
            customer_id,
            group_id,
            COALESCE(
                AVG(
                    CASE
                        WHEN interval_days - Group_Frequency < 0 THEN (
                            interval_days - Group_Frequency
                        ) * (-1)
                        ELSE interval_days - Group_Frequency
                    END / Group_Frequency
                ),
                1
            ) AS Group_Stability_Index
        FROM
            intervals
            JOIN period_view USING (customer_id, group_id)
            JOIN purchase_history_view USING (customer_id, group_id)
        GROUP BY
            customer_id,
            group_id
        ORDER BY customer_id, group_id
    ),
    discount_info AS (
        WITH
            d_count AS (
                SELECT
                    customer_id,
                    group_id,
                    COUNT(transaction_id) over (
                        PARTITION BY
                            customer_id,
                            group_id
                    ) AS dis_count,
                    sku_discount
                FROM
                    customers c
                    JOIN customer_cards cc USING (customer_id)
                    JOIN transactions t USING (customer_card_id)
                    JOIN checks ch USING (transaction_id)
                    JOIN product_matrix pm USING (sku_id)
                WHERE
                    sku_discount > 0
                GROUP BY
                    customer_id,
                    group_id,
                    transaction_id,
                    sku_discount
            ),
            d_share AS (
                SELECT
                    customer_id,
                    group_id,
                    (
                        CAST(dis_count AS FLOAT) / Group_Purchase
                    ) AS Group_Discount_Share
                FROM d_count
                    JOIN period_view USING (customer_id, group_id)
                GROUP BY
                    customer_id,
                    group_id,
                    dis_count,
                    Group_Purchase
            ),
            min_d AS (
                SELECT
                    customer_id,
                    group_id,
                    MIN(Group_Min_Discount) AS Group_Minimum_Discount
                FROM period_view
                WHERE
                    Group_Min_Discount > 0
                GROUP BY
                    customer_id,
                    group_id
            ),
            aver_d AS (
                SELECT
                    customer_id,
                    group_id,
                    (
                        SUM(Group_Summ_Paid) / SUM(Group_Summ)
                    )::NUMERIC AS Group_Average_Discount
                FROM purchase_history_view
                    JOIN checks USING (transaction_id)
                WHERE
                    sku_discount > 0
                GROUP BY
                    customer_id,
                    group_id
            )
        SELECT
            customer_id,
            group_id,
            Group_Discount_Share,
            Group_Minimum_Discount,
            Group_Average_Discount
        FROM aver_d
            JOIN d_share USING (customer_id, group_id)
            JOIN min_d USING (customer_id, group_id)
        GROUP BY
            customer_id,
            group_id,
            Group_Discount_Share,
            Group_Minimum_Discount,
            Group_Average_Discount
    )
SELECT
    customer_id,
    group_id,
    Group_Affinity_Index,
    Group_Churn_Rate,
    Group_Stability_Index,
    Group_Margin,
    Group_Discount_Share,
    Group_Minimum_Discount,
    Group_Average_Discount
FROM
    affinity_index
    LEFT JOIN churn_rate USING (customer_id, group_id)
    LEFT JOIN consumption_stability USING (customer_id, group_id)
    LEFT JOIN discount_info USING (customer_id, group_id)
    RIGHT JOIN customers c USING (customer_id)
ORDER BY customer_id, group_id;
    ----------------------------- Create materialized view:Group_View-----------------------------------------

SELECT *
FROM groups_view;
    ----------------------------- Test -----------------------------