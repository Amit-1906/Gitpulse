with source as (
    select * from {{ source('raw_data', 'raw_commits') }}
)

select
    sha,
    source_repo as repo_name,
    coalesce(payload->'author'->>'login', payload->'commit'->'author'->>'name', 'Unknown') as author_login,
    (payload->'commit'->'author'->>'date')::timestamptz as commit_date,
    payload->'commit'->>'message' as message,
    length(payload->'commit'->>'message') as message_length
from source
