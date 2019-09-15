{{config(materialized='incremental', schema='VLT', enabled=true, tags=['static', 'incremental'])}}

{%- set src_pk = ['CUSTOMER_PK']                                          -%}
{%- set src_nk = ['CUSTOMERKEY']                                          -%}
{%- set src_ldts = 'LOADDATE'                                             -%}
{%- set src_source = 'SOURCE'                                             -%}

{%- set tgt_cols = ['CUSTOMER_PK', 'CUSTOMERKEY', 'SOURCE', 'LOADDATE']   -%}
{%- set tgt_pk = ['CUSTOMER_PK', 'BINARY(16)', 'CUSTOMER_PK']             -%}
{%- set tgt_nk = ['CUSTOMERKEY', 'NUMBER(38,0)', 'CUSTOMERKEY']           -%}
{%- set tgt_ldts = ['LOADDATE', 'DATE', 'LOADDATE']                       -%}
{%- set tgt_source = ['SOURCE', 'VARCHAR(4)', 'SOURCE']                   -%}

{%- set hash_model = [ref('stg_orders_hashed')]                           -%}

{{ snow_vault.hub_template(src_pk, src_nk, src_ldts, src_source,
                           tgt_cols, tgt_pk, tgt_nk, tgt_ldts, tgt_source,
                           src_table, hash_model) }}






