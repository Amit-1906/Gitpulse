with source as (
    select * from {{ source('raw_data', 'raw_pulls') }}
)

select
    pr_number as number,
    source_repo as repo_name,
    payload->>'title' as title,
    payload->>'state' as state,
    coalesce(payload->'user'->>'login', 'ghost') as author_login,
    (payload->>'created_at')::timestamptz as created_at,
    (payload->>'merged_at')::timestamptz as merged_at,
    (payload->>'merged_at' is not null)::boolean as is_merged,
    case
        when payload->>'merged_at' is not null then
            round((extract(epoch from (payload->>'merged_at')::timestamptz - (payload->>'created_at')::timestamptz) / 3600.0)::numeric, 2)
        else null
    end as hours_to_merge
from source
