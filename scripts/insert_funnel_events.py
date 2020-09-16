import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({'create_source': [0, 1, 2],
                   'day': ['2020-09-16', '2020-09-15', '2020-09-15'],
                   'hour': ['01', '12', '23'],
                   'minute': ['45', '12', '1'],
                   'pt': ['adx', 'as', 'bs']})
fields = [
    pa.field('create_source', pa.int32()),
    pa.field('day', pa.string()),
    pa.field('hour', pa.string()),
    pa.field('minute', pa.string()),
    pa.field('pt', pa.string()),
]
my_schema = pa.schema(fields)
table = pa.Table.from_pandas(df, schema=my_schema)
pq.write_table(table, 'cpc_basedata_funnel_events.parquet2')
table2 = pq.read_table('cpc_basedata_funnel_events.parquet2')
print(table2)
print(table2.to_pandas())

"""
create table `dl_cpc.cpc_basedata_funnel_events_temp` (
`create_source` int,
`day` string,
`hour` string,
`minute` string,
`pt` string
)
STORED AS PARQUET;

-- 将本地文件加载到临时表
load data local inpath './cpc_basedata_funnel_events.parquet2' overwrite into table dl_cpc.cpc_basedata_funnel_events_temp;

set hive.exec.dynamic.partition.mode=nonstrict;

-- 将临时表的数据插入到dl_cpc.cpc_basedata_funnel_events
insert into table dl_cpc.cpc_basedata_funnel_events 
partition (day, hour, minute, pt)(create_source, day, hour, minute, pt)
select create_source, day, hour, minute, pt 
from dl_cpc.cpc_basedata_funnel_events_temp;
"""
