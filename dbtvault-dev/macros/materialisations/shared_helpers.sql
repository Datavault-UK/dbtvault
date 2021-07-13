{%- macro check_placeholder(model_sql, placeholder='__PERIOD_FILTER__') -%}

    {%- if model_sql.find(placeholder) == -1 -%}
        {%- set error_message -%}
            Model '{{ model.unique_id }}' does not include the required string '{{ placeholder }}' in its sql
        {%- endset -%}
        {{- exceptions.raise_compiler_error(error_message) -}}
    {%- endif -%}

{%- endmacro -%}


{%- macro is_any_incremental() -%}
    {%- if dbtvault.is_vault_insert_by_period() or dbtvault.is_vault_insert_by_rank() or is_incremental() -%}
        {%- do return(true) -%}
    {%- else -%}
        {%- do return(false) -%}
    {%- endif -%}
{%- endmacro -%}