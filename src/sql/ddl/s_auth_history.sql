CREATE TABLE STV2024111115__DWH.s_auth_history
(
    hk_l_user_group_activity int NOT NULL,
    user_id_from int,
    event varchar(10),
    event_dt timestamp,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_auth_history.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_auth_history.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_auth_history.load_dt)::date))::date WHEN ("datediff"('month', (s_auth_history.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_auth_history.load_dt)::date))::date ELSE (s_auth_history.load_dt)::date END);


ALTER TABLE STV2024111115__DWH.s_auth_history ADD CONSTRAINT fk_s_auth_history_hk_l_user_group_activity FOREIGN KEY (hk_l_user_group_activity) references STV2024111115__DWH.l_user_group_activity (hk_l_user_group_activity);

CREATE PROJECTION STV2024111115__DWH.s_auth_history /*+createtype(P)*/ 
(
 hk_l_user_group_activity,
 user_id_from,
 event,
 event_dt,
 load_dt,
 load_src
)
AS
 SELECT s_auth_history.hk_l_user_group_activity,
        s_auth_history.user_id_from,
        s_auth_history.event,
        s_auth_history.event_dt,
        s_auth_history.load_dt,
        s_auth_history.load_src
 FROM STV2024111115__DWH.s_auth_history
 ORDER BY s_auth_history.load_dt
SEGMENTED BY s_auth_history.hk_l_user_group_activity ALL NODES KSAFE 1;