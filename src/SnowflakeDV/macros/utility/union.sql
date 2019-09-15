{%- macro union(src_pk, src_nk, src_ldts, src_source, tgt_cols, tgt_pk, src_table, hash_model) -%}

    SELECT {{ tgt_cols|join(", ") }},
    LAG({{ src_source }}, 1)
    OVER(PARTITION by {{ tgt_pk }}
    ORDER BY {{ tgt_pk }}) AS FIRST_SOURCE
    FROM (

 {%- set letters='abcdefghijklmnopqrstuvwxyz' -%}

      {%- for src in src_table %}

      {%- set letter = letters[loop.index0] %}
      {{ snow_vault.single(src_pk[loop.index0], src_nk[loop.index0], src_ldts, src_source,
                            tgt_pk,
                            src_table[loop.index0] or none, hash_model[loop.index0] or none,
                            letter,
                            union=true) -}}
      {%- if not loop.last %}
      UNION
      {%- endif -%}
      {%- endfor -%})
{%- endmacro -%}