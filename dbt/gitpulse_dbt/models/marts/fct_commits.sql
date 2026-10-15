{{ config(materialized='table') }}

with commits as (
    select * from {{ ref('stg_commits') }}
)

select
    sha,
    repo_name,
    author_login,
    commit_date,
    message,
    message_length
from commits
