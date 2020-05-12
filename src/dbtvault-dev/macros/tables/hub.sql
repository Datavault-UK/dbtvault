{#- Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-#}

{%- macro hub(src_pk, src_nk, src_ldts, src_source, source_model) -%}

{%- set source_cols = dbtvault.get_src_col_list([src_pk, src_nk, src_ldts, src_source]) -%}

-- Generated by dbtvault.
WITH

{%- if source_model is iterable and source_model is not string %}

{% for src in source_model %}

STG_{{ loop.index|string }} AS (
    SELECT DISTINCT
      {{ dbtvault.prefix(source_cols, 'a') }}
    FROM (
    SELECT
    {{ src_pk }}, {{ src_nk }}, {{ src_ldts }}, {{ src_source }},
    ROW_NUMBER() OVER(PARTITION BY {{ src_pk }} ORDER BY {{ src_ldts }} ASC) AS RN
    FROM {{ ref(src) }}
    ) AS a
    WHERE RN = 1),
{% endfor %}

STG AS (
    SELECT DISTINCT
      {{ dbtvault.prefix(source_cols, 'b') }}
    FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY {{ src_pk }} ORDER BY {{ src_ldts }}, {{ src_source }} ASC) AS RN
    FROM (
    {%- for src in source_model %}
    SELECT * FROM
    {%- if loop.index == source_model|length %}
    STG_{{ loop.index|string }}
    {%- else %}
    STG_{{ loop.index|string }}
    UNION
    {%- endif %}
    {%- endfor %}
    )
    WHERE {{ src_pk }}<>{{ dbtvault.hash_check("^^") }}
    AND {{ src_pk }}<>{{ dbtvault.hash_check("") }}) AS b
    WHERE RN = 1)

{%- else %}

STG AS (
    SELECT DISTINCT
      {{ dbtvault.prefix(source_cols, 'a') }}
    FROM (
    SELECT b.*,
    ROW_NUMBER() OVER(PARTITION BY {{ dbtvault.prefix([src_pk], 'b') }}
    ORDER BY {{ dbtvault.prefix([src_ldts], 'b') }}, {{ dbtvault.prefix([src_source], 'b') }} ASC) AS RN
    FROM {{ ref(source_model) }} AS b
    WHERE {{ dbtvault.prefix([src_pk], 'b') }}<>{{ dbtvault.hash_check("^^") }}
    AND {{ dbtvault.prefix([src_pk], 'b') }}<>{{ dbtvault.hash_check("") }}) AS a
    WHERE RN = 1)

{%- endif %}

SELECT c.* FROM STG AS c
{%- if is_incremental() %}
LEFT JOIN {{ this }} AS d ON {{ dbtvault.prefix([src_pk], 'c') }}={{ dbtvault.prefix([src_pk], 'd') }}
WHERE {{ dbtvault.prefix([src_pk], 'd') }} IS NULL
{%- endif -%}

{%- endmacro -%}