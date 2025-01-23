WITH user_group_log AS 
(
SELECT
	hg.hk_group_id,
	hg.registration_dt,
	count(DISTINCT luga.hk_user_id) AS cnt_added_users
FROM
	STV2024111115__DWH.h_groups hg
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
), 
user_group_messages AS 
(
SELECT
	lgd.hk_group_id,
	count(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
FROM
	STV2024111115__DWH.l_groups_dialogs AS lgd
LEFT JOIN STV2024111115__DWH.l_user_message AS lum ON
	lum.hk_message_id = lgd.hk_message_id
GROUP BY
	lgd.hk_group_id
) 
SELECT
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users group_conversion
FROM
	user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON
	ugl.hk_group_id = ugm.hk_group_id
ORDER BY
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;