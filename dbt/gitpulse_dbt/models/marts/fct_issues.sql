{{ config(materialized='table') }}

with issues as (
    select * from {{ ref('stg_issues') }}
)

select
    number,
    repo_name,
    title,
    state,
    author_login,
    created_at,
    closed_at,
    is_pr,
    days_to_close
from issues
