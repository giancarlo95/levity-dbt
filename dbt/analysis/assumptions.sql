WITH button_clicked AS (

    SELECT 
        DISTINCT anonymous_id
    FROM 
        {{ref('button_clicked_view')}}
    WHERE 
        action="Get Started"

), pages AS (

    SELECT 
        DISTINCT anonymous_id
    FROM 
        {{ref('pages_view')}}
    WHERE 
        title LIKE "%getting-started-1%"

)

SELECT 
    *
FROM pages INNER JOIN button_clicked
    ON pages.anonymous_id=button_clicked.anonymous_id