drop function if exists main;
drop function if exists PeriodOne;
drop function if exists PeriodTwo;
drop function if exists reward;

-- переводим формат даты в нужный формат

set datestyle = 'ISO, DMY';

-- методика расчета по периоду
create or replace function PeriodOne(
    DatePeriod varchar,
    AverageBill_increaseIndex real
)
    returns table(
                     customer_id bigint,
                     average_check_target real
                 )
    language plpgsql as
$$
declare
    BeginDate date = split_part(DatePeriod, ' ', 1)::date;
    EndDate date = split_part(DatePeriod, ' ', 2)::date;
begin
    if (BeginDate is null or EndDate is null or EndDate <= BeginDate) then
        raise exception 'Last date of the specified period must be later than the first date. Also dates can not be null';
    end if;
    return query
        with clients as (
            select cc.customer_id,
                   tr.transaction_summ
            from transactions tr
                join customer_cards cc on cc.customer_card_id = tr.customer_card_id
            where tr.transaction_date_time between BeginDate and EndDate
        )
        select clients.customer_id,
               ((sum(clients.transaction_summ) / count(*)) * AverageBill_increaseIndex)::real as average_check_target from clients
        group by clients.customer_id, AverageBill_increaseIndex;
end;
$$;

-- select * from PeriodOne('01-01-2018 31-12-2022', 1.2);

-- методику расчета по количеству последних транзакций
create or replace function PeriodTwo(
    NumTransactions bigint,
    AverageBill_increaseIndex real
)
    returns table (
                      customer_id bigint,
                      average_check_target real
                  )
    language plpgsql as
$$
begin
    return query
        with clients as (
            select cc.customer_id,
                   tr.transaction_summ,
                   row_number() over (partition by cc.customer_id order by tr.transaction_summ desc)
            from transactions tr
                join customer_cards cc on cc.customer_card_id = tr.customer_card_id
        )
        select clients.customer_id, ((sum(clients.transaction_summ) / count(*)) * AverageBill_increaseIndex) ::real AS average_check_target
        from clients
        group by clients.customer_id, AverageBill_increaseIndex;
end;
$$;

select * from PeriodTwo(8, 1.2);


create or replace function GroupException (
    MaxChurnRate real,
    MaxShareTransactions real,
    MarginPercentage real
)
     returns table(
                    Customer_ID bigint,
                    Group_ID bigint,
                    Offer_Discount_Depth numeric
                  )
as $$
begin
    return query
        with tmp1 as (
                    -- выбирает записи из таблицы Groups, где показатель оттока меньше MaxChurnIndex и доля скидок меньше MaxShareTransactions.
                    select g.group_churn_rate,
                           g.group_discount_share,
                           g.customer_id::bigint,
                           g.group_id::bigint,
                           g.group_margin,
                           g.group_affinity_index
                    from groups_view g
                    where g.group_churn_rate < MaxChurnRate
                      and g.group_discount_share < MaxShareTransactions / 100
                    ),
            tmp2 as (
                     -- вычисляет сумму покупок и затрат для каждого клиента в каждой группе.
                     select p.customer_id,
                            p.group_id,
                            sum(p.group_summ-p.group_cost)/sum(p.group_summ) as Offer_Discount_Depth
                     from purchase_history_view p
                     group by p.customer_id, p.group_id
                    ),
            tmp3 as (
                    -- соединяет таблицы tmp1 и tmp2, а затем выбирает записи, где минимальная скидка в периоде меньше,
                    -- чем MarginPercentage от предложенной скидки.
                     select tmp1.*,
                            ceil(per.group_min_discount / 0.05) * 5 as Offer_Discount_Depth
                     from tmp1
                        join tmp2 on  tmp2.customer_id = tmp1.customer_id and tmp2.group_id = tmp1.group_id
                        join (select p.customer_id,
                                     p.group_id,
                                     p.group_min_discount
                              from period_view p) as per on per.customer_id = tmp1.customer_id and per.group_id = tmp1.group_id
                     where ceil(per.group_min_discount / 0.05) * 0.05  < tmp2.Offer_Discount_Depth * MarginPercentage / 100
                    ),
            tmp4 as (
                     -- добавляет столбец firstval, который содержит первое значение group_affinity_index для каждого клиента.
                     -- выбирает записи из tmp4, где firstval равен group_affinity_index, и возвращает их.
                     select tmp3.customer_id::bigint,
                            tmp3.group_id::bigint,
                            tmp3.group_affinity_index,
                            tmp3.Offer_Discount_Depth,
                            first_value(tmp3.group_affinity_index) over (
                                partition by tmp3.customer_id order by tmp3.group_affinity_index desc ) as firstval
                     from tmp3
                    )
        select tmp4.customer_id::bigint,
               tmp4.group_id::bigint,
               tmp4.Offer_Discount_Depth
        from tmp4
        where tmp4.firstval = tmp4.group_affinity_index;
end;
$$ language plpgsql;

create or replace function main (
    AverageCheckMethod integer,
    DatePeriod varchar,
    NumOfTransactions bigint,
    AverageBill_increaseFactor numeric,
    MaxChurnIndex integer,
    MaxShareTransactions integer,
    MarginPercentage integer
)
    returns table(
                      Customer_ID bigint,
                      Required_Check_Measure real,
                      Group_Name varchar,
                      Offer_Discount_Depth real
                  )
    language plpgsql as
$$
begin
    if (AverageCheckMethod = 1) then
        return query select period_date.Customer_ID::bigint,
                            period_date.average_check_target::real as Required_Check_Measure,
                            sku_groups.group_name::varchar,
                            GroupException.Offer_Discount_Depth::real
                     from PeriodOne(DatePeriod, AverageBill_increaseFactor) AS period_date
                              join GroupException(MaxChurnIndex, MaxShareTransactions, MarginPercentage) GroupException on period_date.customer_id = GroupException.customer_id
                              join sku_groups on sku_groups.group_id = GroupException.Group_ID
                     order by 1;
    elseif (AverageCheckMethod = 2) then
        return query select period_transactions.Customer_ID::bigint,
                            period_transactions.average_check_target::real as Required_Check_Measure,
                            sku_groups.group_name::varchar,
                            GroupException.Offer_Discount_Depth::real
                     from PeriodTwo(NumOfTransactions, AverageBill_increaseFactor) as period_transactions
                              join GroupException(MaxChurnIndex, MaxShareTransactions, MarginPercentage) GroupException on period_transactions.customer_id = GroupException.Customer_ID
                              join sku_groups on sku_groups.group_id = GroupException.Group_ID
                     order by 1;
    else
        raise exception 'Не правильно выбран метод расчета среднего чека (1–по периоду или 2–по количеству)';
    end if;
end;
$$;

set datestyle = 'ISO, DMY';

-- Проверка работы
SELECT * from main(1, '19-03-2018 25-01-2020', 100,  1.15, 3, 70, 30);
SELECT * from main(1, '19-03-2018 25-01-2020', 50,  1.1, 7, 100, 50);
SELECT * from main(1, '19-03-2018 25-01-2020', 30,  1.2, 0, 0, 0);
SELECT * from main(2, '01-20-2018 08-20-2022', 100,  1.15, 3, 70, 30);
SELECT * from main(2, '01-20-2018 08-20-2022', 50,  1.1, 7, 100, 50);
SELECT * from main(2, '01-20-2018 08-20-2022', 30,  1.2, 0, 0, 0);


