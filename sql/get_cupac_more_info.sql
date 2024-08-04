with main_data as (
    select install_pk,  sum(num_game_start) as num_game_start, platform, country_code, device_type
    from f_user_activity 
    where application='solitaire' and dt between '2024-04-01' and '2024-04-07' and activity_type='event' 
    group by install_pk, platform, country_code, device_type
    order by install_pk
),

min_cohort_day as(
    select install_pk, min(cohort_day) as cohort_day
    from f_user_activity 
    where application='solitaire' and dt between '2024-04-01' and '2024-04-07' and activity_type='event' 
    group by install_pk
    order by install_pk
),

pre_game_start as (
    select install_pk,  sum(num_game_start) as pre_num_game_start, sum(num_game_won) as pre_num_game_won
    from f_user_activity
    where application='solitaire' and dt between '2024-03-18' and '2024-03-31' and activity_type='event' 
    group by install_pk
    order by install_pk
),

final_data as (
    select fin.*, f_installs.cpi,  f_installs.install_date, f_installs.source
    from(
        select gs.*, min_cohort_day.cohort_day
        from(
            select main_data.*, pre_game_start.pre_num_game_start, pre_game_start.pre_num_game_won
            from main_data
            left join pre_game_start on main_data.install_pk = pre_game_start.install_pk
        )gs
        join min_cohort_day on min_cohort_day.install_pk = gs.install_pk  
    )fin
    left join f_installs on f_installs.install_pk = fin.install_pk
    order by fin.install_pk 
)

select * from final_data
order by install_pk
