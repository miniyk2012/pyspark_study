参考书: https://github.com/databricks/learning-spark



## 生产导入CDH环境大数据工具:
0. 功能: 支持生产上任意分区表的任意指定列导入到测试表
1. 原理: 从qe查询出要导入的数据, 指定字段类型, 用pyspark读取成dataframe, 调用CDH的hive thrift2接口将dataframe写入测试表, 并自动动态分区
2. prerequisit: 
    * 本地安装好spark-2.4.7 on hadoop2.6
    * pip install pyspark
    * python3.6
3. 操作步骤
    * 在CDH环境建表: dl_cpc.cpc_bd_sdk_show_v1
    * 查询qe, 下载为csv文件, 放到files/load_prod_data/中, cpc_bd_sdk_show_v1.csv:
    
    ```csv
    searchid,src,slotid,mediaid,dsp_show,day,hour,mm
    rqHT1BVfUHasTyHXd2Vrm7ayZhFKGdqPdBhz4-H7,ADX,7481000,80000001,2,2020-09-25,12,10
    ...
    ```
    
    * 编写对应字段的schema配置, 放到files/load_prod_data/中, cpc_bd_sdk_show_v1_schema.json:
    ```json
    {
      "searchid": "string",
      "src": "string",
      "slotid": "string",
      "mediaid": "string",
      "dsp_show": "smallint",
      "day": "string",
      "hour": "string",
      "mm": "string"
    }
    ```
    * cd awesome/job/, 找到`load_prod_data_job.py`文件, 传入以下参数:
        1. file_path: cpc_bd_sdk_show_v1.csv的绝对路径
        2. dfs_file_path: CDH环境的目标表目录, hdfs://zjkb-cpc-backend-bigdata-qa-01:8020/user/hive/warehouse/dl_cpc.db/cpc_basedata_bidsdk_event/event_type=show
        3. schema_path: cpc_bd_sdk_show_v1_schema.json的绝对路径
        4. database: CDH的目标数据库, dl_cpc
        5. table: CDH的目标表cpc_bd_sdk_show_v1
        6. partition_columns: CDH目标表的分区字段, ['day', 'hour', 'mm']
    * 点击Run, 运行main代码, 导入成功
4. 注意点
    1. 目前不支持MAP, ARRAY, STRUCT类型字段, 原因是csv文件格式有二义性, 无法区分列还是元素
    2. 列的值不能包含逗号, 原因同上
    3. 必须将分区字段按从大到小的粒度放在列的最后
    4. 若有环境问题本地跑不通的, 找杨恺解决
    
5. 使用情况
    
    目前该工具已通过杨恺的多次实战, 导入过cpc_bd_sdk_show_v1表和cpc_basedata_click_event表的生产数据, 用于主题表的本地测试: dl_cpc.dws_cpc_bd_sdk_show_daily的实战(http://corp-dc-kepler.qutoutiao.net/kepler-vue/#/?type=Hive%20SQL&name=dl_cpc.dws_cpc_bd_sdk_show_daily), 欢迎大家尝试使用, 也可以作为测试同学提高的学习模板
