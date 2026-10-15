{{ config(materialized='table') }}

with pulls as (
    select * from {{ ref('stg_pulls') }}
)

select
    number,
    repo_name,
    title,
    state,
    author_login,
    created_at,
    merged_at,
    is_merged,
    hours_to_merge
from pulls
