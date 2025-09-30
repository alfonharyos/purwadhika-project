

with calendar as (
    -- generate semua tanggal dari 2023-01-01 s/d akhir tahun berjalan
    select day as date
    from unnest(
        generate_date_array(
            date('2023-01-01'),
            last_day(current_date(), year),
            interval 1 day
        )
    ) as day
),
weekend_days as (
    -- hanya Jumat(6), Sabtu(7), Minggu(1)
    select
        date,
        case extract(dayofweek from date)
            when 6 then 'Friday'
            when 7 then 'Saturday'
            when 1 then 'Sunday'
        end as holiday_type
    from calendar
    where extract(dayofweek from date) in (6,7,1)
),
hours as (
    -- generate jam 0–23
    select hour
    from unnest(generate_array(0, 23, 1)) as hour
),
holiday_with_hours as (
    select
        d.date,
        d.holiday_type,
        h.hour,
        timestamp_add(timestamp(d.date), interval h.hour hour) as holiday_hour
    from weekend_days d
    cross join hours h
)

select * from holiday_with_hours