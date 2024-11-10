imf_weo_csv_parquet.py
input: xls file inside IMF_WEO_IN, obtained from IMF website
output: cy_imf_weo.parquet
remark: imf data might change frequently, this file needs to be monitored constantly

xmart_parquet.py
input: table name of wiise xmart
output: TABLE_NAME_year.parquet
remark: start and end year might need to be adjusted for new data

imf_weo_required_columns.py
input: cy_imf_weo.parquet
output: cy_ie.parquet
remark: this file creates LCU, USD_percapita, DTPCV1

whdh_gold_data.py
input: all parquet files
output: all json files inside whdh_gold
remark: this file creates json file for charts from all parquet files

csv inside gni is obtained manually
