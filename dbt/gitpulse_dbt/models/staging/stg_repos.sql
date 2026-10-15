with source as (
    select * from {{ source('raw_data', 'raw_repos') }}
)

select
    source_repo as repo_name,
    (payload->>'stargazers_count')::integer as stars,
    (payload->>'forks_count')::integer as forks,
    (payload->>'open_issues_count')::integer as open_issues,
    payload->>'language' as language,
    (payload->>'created_at')::timestamptz as created_at
from source
