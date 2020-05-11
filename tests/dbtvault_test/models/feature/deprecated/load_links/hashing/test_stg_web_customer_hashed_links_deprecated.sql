{{- config(materialized='table', schema='stg', enabled=true, tags=['load_links', 'deprecated']) -}}

{%- set source_table = source('test_deprecated', 'stg_web_customer_deprecated')                          %}

{{ dbtvault.multi_hash([('CUSTOMER_REF', 'CUSTOMER_PK'),
                         ('NATION_KEY', 'NATION_PK'),
                         (['CUSTOMER_REF', 'NATION_KEY'], 'CUSTOMER_NATION_PK')])  }},

{{ dbtvault.add_columns(source_table)                                              }}

{{- dbtvault.from(source_table)                                                    }}

