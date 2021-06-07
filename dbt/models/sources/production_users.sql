select
    au.id as user_id,
	au.email as user_email_address,
	au.date_joined as date_user_onboarded
from
	{{ source('google_cloud_postgresql_public', 'auth_user') }} au
left join {{ source('google_cloud_postgresql_public', 'accounts_userprofile') }} au2 on
	au.id = au2.user_id
where
	is_staff = false
	and is_approved = true
	and not(email like '%@angularventures.com')
	and not(email like '%@discovery-ventures.com')
	and not(email like '%@levity.ai')
    and not(email='abcaisodjaiosdjioasd@gmail.com')
    and not(email='adil.islam619@gmail.com')
    and not(email='milanjose999@gmail.com')
    and not(email='hanna.kleinings@gmail.com')