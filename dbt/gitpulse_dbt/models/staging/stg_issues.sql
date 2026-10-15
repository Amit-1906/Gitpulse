with source as (
    select * from {{ source('raw_data', 'raw_issues') }}
)

select
    issue_number as number,
    source_repo as repo_name,
    payload->>'title' as title,
    payload->>'state' as state,
    coalesce(payload->'user'->>'login', 'ghost') as author_login,
    (payload->>'created_at')::timestamptz as created_at,
    (payload->>'closed_at')::timestamptz as closed_at,
    (payload ? 'pull_request')::boolean as is_pr,
    case
        when payload->>'closed_at' is not null then
            round((extract(epoch from (payload->>'closed_at')::timestamptz - (payload->>'created_at')::timestamptz) / 86400.0)::numeric, 2)
        else null
    end as days_to_close
from source
