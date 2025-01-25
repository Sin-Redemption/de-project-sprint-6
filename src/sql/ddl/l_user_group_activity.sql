DROP TABLE IF EXISTS STV2024111115__DWH.l_user_group_activity;
CREATE TABLE STV2024111115__DWH.l_user_group_activity
(
    hk_l_user_group_activity int NOT NULL,
    hk_user_id int NOT NULL,
    hk_group_id int NOT NULL,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_user_group_activity) DISABLED
)
PARTITION BY ((l_user_group_activity.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (l_user_group_activity.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (l_user_group_activity.load_dt)::date))::date WHEN ("datediff"('month', (l_user_group_activity.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (l_user_group_activity.load_dt)::date))::date ELSE (l_user_group_activity.load_dt)::date END);


ALTER TABLE STV2024111115__DWH.l_user_group_activity ADD CONSTRAINT fk_l_user_group_activity_user FOREIGN KEY (hk_user_id) references STV2024111115__DWH.h_users (hk_user_id);
ALTER TABLE STV2024111115__DWH.l_user_group_activity ADD CONSTRAINT fk_l_user_group_activity_message FOREIGN KEY (hk_group_id) references STV2024111115__DWH.h_groups (hk_group_id);

CREATE PROJECTION STV2024111115__DWH.l_user_group_activity /*+createtype(P)*/ 
(
 hk_l_user_group_activity,
 hk_user_id,
 hk_group_id,
 load_dt,
 load_src
)
AS
 SELECT l_user_group_activity.hk_l_user_group_activity,
        l_user_group_activity.hk_user_id,
        l_user_group_activity.hk_group_id,
        l_user_group_activity.load_dt,
        l_user_group_activity.load_src
 FROM STV2024111115__DWH.l_user_group_activity
 ORDER BY l_user_group_activity.load_dt
SEGMENTED BY l_user_group_activity.hk_user_id ALL NODES KSAFE 1;