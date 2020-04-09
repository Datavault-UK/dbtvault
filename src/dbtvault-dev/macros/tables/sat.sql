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

{%- macro sat(src_pk, src_hashdiff, src_payload,
              src_eff, src_ldts, src_source,
              source) -%}

{%- set source_cols = dbtvault.get_src_col_list([src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source]) -%}

-- Generated by dbtvault.
SELECT DISTINCT {{ dbtvault.alias_all(source_cols, 'e') }}
FROM {{ ref(source) }} AS e
{% if is_incremental() -%}
LEFT JOIN (
    SELECT {{ dbtvault.prefix(source_cols, 'd', alias_target='target') }}
    FROM (
          SELECT {{ dbtvault.prefix(source_cols, 'c', alias_target='target') }},
          CASE WHEN RANK()
          OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'c') }}
          ORDER BY {{ dbtvault.prefix([src_ldts], 'c') }} DESC) = 1
          THEN 'Y' ELSE 'N' END CURR_FLG
          FROM (
            SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='target') }}
            FROM {{ this }} as a
            JOIN {{ ref(source) }} as b
            ON {{ dbtvault.prefix([src_pk], 'a') }} = {{ dbtvault.prefix([src_pk], 'b') }}
          ) as c
    ) AS d
WHERE d.CURR_FLG = 'Y') AS src
ON {{ dbtvault.prefix([src_hashdiff], 'src') }} = {{ dbtvault.prefix([src_hashdiff], 'e') }}
WHERE {{ dbtvault.prefix([src_hashdiff], 'src') }} IS NULL
{%- endif -%}

{% endmacro %}
