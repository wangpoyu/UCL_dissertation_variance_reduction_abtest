set (start_date, end_date) = (
    select
        experiment_start_at::date
        ,coalesce(archived_at::date, experiment_end_at::date)
    from tds_db.public.f_experiment_variants
    where
        experiment_id = {experiment_id}
        and source = {server}
    group by 1,2
);

with f_experiment_users as (
    select
        u.install_pk
        ,v.variant_name
        ,v.variant_default
        ,u.assigned_at

        ,coalesce(u.num_game_start, 0) as num_game_start
        ,coalesce(u.banner_rev, 0) + coalesce(u.interstitial_rev, 0) + coalesce(u.rewarded_rev, 0) + coalesce(u.transactions_rev, 0) as total_revenue
        ,coalesce(u.num_ad_impression_rewarded, 0) as num_ad_impression_rewarded
        ,coalesce(u.num_ad_impression_interstitial, 0) as num_ad_impression_interstitial
        ,coalesce(u.num_ad_impression_banner, 0) as num_ad_impression_banner

    from tds_db.public.f_experiment_users as u
    left join tds_db.public.f_experiment_variants v
        on u.variant_id = v.variant_id
        and u.experiment_id = v.experiment_id
        and u.exper_source = v.source
    where
        u.experiment_id = {experiment_id}
        and u.exper_source = {server}
        -- and u.platform = 'android'
        -- and u.install_country in ('US', 'GB', 'AU', 'CA')
)


,pre_experiment_activity as (
    select
        fua.install_pk
        ,dt
        ,datediff('day', u.assigned_at, fua.dt) as cohort_day --how many days the user has been assigned to the experiment

        ,iff(activity_type = 'event', num_game_start, 0) as num_game_start
        ,iff(activity_type = 'event', num_ad_impression_rewarded, 0) as num_ad_impression_rewarded
        ,iff(activity_type = 'event', num_ad_impression_interstitial, 0) as num_ad_impression_interstitial
        ,iff(activity_type = 'event', num_ad_impression_banner, 0) as num_ad_impression_banner
        ,iff(
            activity_type = 'revenue', 
            coalesce(sum_interstitial_revenue, 0) + coalesce(sum_banner_revenue, 0) + coalesce(sum_rewarded_revenue, 0) + coalesce(sum_transactions_revenue, 0),
            0
        ) as total_revenue
        ,iff(activity_type = 'event', num_game_won, 0) as num_game_won
        
    from tds_db.public.f_user_activity fua
    inner join (select install_pk, assigned_at from f_experiment_users) u
        on fua.install_pk = u.install_pk
        and fua.dt between dateadd('day', -{lookback}, u.assigned_at) and dateadd('day', -1, u.assigned_at)
    where   
        fua.application = {app}
        and fua.dt between dateadd('day', -{lookback}, $start_date) and $end_date
        -- and fua.platform = 'android'
        
)


,aggregated_pre_exp_user_data as (
    select
        pre_experiment_activity.install_pk

        ,sum(num_game_start) as pre_num_game_start
        ,sum(total_revenue) as pre_total_revenue
        ,sum(num_ad_impression_rewarded) as pre_num_ad_impression_rewarded
        ,sum(num_ad_impression_interstitial) as pre_num_ad_impression_interstitial
        ,sum(num_ad_impression_banner) as pre_num_ad_impression_banner
        ,sum(num_game_won) as pre_num_game_won

    from pre_experiment_activity 
    group by install_pk
    order by install_pk
)

,final_data as (
    select main.*, f_installs.install_date
    ,datediff('day', f_installs.install_date, main.assigned_at) as cohort_day_install
    ,f_installs.platform, f_installs.country_code, f_installs.device_type, f_installs.cpi, f_installs.source

    from(
        select exp_data.*, pre_data.pre_num_game_start
        , pre_data.pre_total_revenue
        , pre_data.pre_num_ad_impression_rewarded
        , pre_data.pre_num_ad_impression_interstitial
        , pre_data.pre_num_ad_impression_banner
        , pre_num_game_won
        from f_experiment_users exp_data
        left join aggregated_pre_exp_user_data pre_data 
            using (install_pk)
    )main
    left join f_installs on main.install_pk = f_installs.install_pk
    order by main.install_pk
)

select * from final_data 
order by install_pk
