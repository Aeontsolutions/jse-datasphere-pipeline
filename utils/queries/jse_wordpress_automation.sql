with 
	inst as (select
		i.InstrumentID
        ,it.term_id
        ,it.slug
		,i.InstrumentCode
		,i.InstrumentName
        ,irel.*
	from 
		portal_repo_Instrument i
		join jseweb_terms it on it.name = i.InstrumentCode
        join jseweb_term_taxonomy itax on itax.term_id = it.term_id and itax.taxonomy = 'post_tag'
		join jseweb_term_relationships irel on irel.term_taxonomy_id = itax.term_taxonomy_id
	 order by object_id
	),
    
    cat as (select
		catt.name
        ,catt.slug
        ,catt.term_id
        ,crel.object_id
    from 
		jseweb_terms catt
		join jseweb_term_taxonomy cattax on cattax.term_id = catt.term_id 
			and cattax.taxonomy = 'category'
		join jseweb_term_relationships crel on crel.term_taxonomy_id = cattax.term_taxonomy_id
	where
		catt.slug in (
			'annual-reports', 
            'audited-financial-statements', 
            'quarterly-financial-statements'
		)
    )
select
	a.id
    ,inst.instrumentcode
    ,a.post_title
    ,a.post_name
    ,a.guid
    ,cat.name
    ,a.post_date
    ,p.post_status
from 
	jseweb_posts a
    join jseweb_posts p on a.post_parent = p.id
    join inst on inst.object_id = p.id
    join cat on cat.object_id = inst.object_id
where
	a.post_type = 'attachment'
	and a.post_mime_type in ('application/pdf', 'application/msword')
    and a.post_date > '2025-01-01'
order by post_date
;

-- select * from jseweb_posts where id = 21838;
-- select * from jseweb_term_taxonomy;