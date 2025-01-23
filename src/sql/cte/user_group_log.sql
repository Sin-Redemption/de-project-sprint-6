WITH user_group_log AS 
(
SELECT
	hg.hk_group_id,
	hg.registration_dt,
	count(DISTINCT luga.hk_user_id) AS cnt_added_users
FROM
	STV2024111115__DWH.h_groups AS hg
LEFT JOIN STV2024111115__DWH.l_user_group_activity AS luga ON
	luga.hk_group_id = hg.hk_group_id
LEFT JOIN STV2024111115__DWH.s_auth_history AS sah ON
	sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
WHERE
	sah.event = 'add'
GROUP BY
	hg.hk_group_id,
	hg.registration_dt
ORDER BY
	hg.registration_dt
LIMIT 10
) 
SELECT
	hk_group_id,
	cnt_added_users
FROM
	user_group_log
ORDER BY
	cnt_added_users
LIMIT 10;