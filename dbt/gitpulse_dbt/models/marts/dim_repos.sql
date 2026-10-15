{{ config(materialized='table') }}

with repos as (
    select * from {{ ref('stg_repos') }}
)

select
    repo_name,
    stars,
    forks,
    open_issues,
    language,
    created_at
from repos
