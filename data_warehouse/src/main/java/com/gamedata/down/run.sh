#!/bin/sh

startDate=$1
endDate=$2
today=`date -d "" +%Y-%m-%d`

# 线程数
threadCount=20
# MySQL 连接超时时间设置
timeOut=6000;
#使用的游戏
game="hqg";
# 要连接的文件地址，注意： 文件必须以\t制表符分割， 格式为： 平台id	区服id	Ip地址	端口号	用户名	密码	数据库名称
filePath=/home/bigdata/hadoop-project/hqg/tmp/hqg_url_new.txt
# 要拉取数据的mysql 表名
mysqlTable="log_recharge"
# 表名后缀规则，name_2015_1 n_yyyym, name_201501 n_yyyymm, name n
rule="n"
# 要拉取的字段, 使用注意事项，当拉取字段为 * 时，要把area_id,plat字段定义在hive表的最前面
fields="\\*";
# 拉取的条件, 注意，前后要加空格，这个时间要决定分表的月份的，所以如果是分月表必须知道时间，但是可以使用 '2017-01-01'>='${startDate}' and '2001-01-01'<='${endDate}' 这种形式
where=" date(time)>='${startDate}' and date(time)<='${endDate}' "
# 下载的数据的存放目录，不要 / 结尾, 如果路径不存在，要创建
tmpDataPath="/data1/tmp_down"

# 要加载的hive表
hiveTable="ods_log_recharge"
# 要加载的hive临时表
tmphiveTable="tmp_ods_log_recharge"
# 要加载的hive数据库
hiveDatabase="hqg_ods"


java -cp /home/bigdata/hadoop-project/download/mysql-connector-java-5.1.10.jar:/home/bigdata/hadoop-project/download/Skymoons-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.skymoons.down.DownloadData ${threadCount} ${timeOut} ${game} ${filePath} ${mysqlTable} ${rule} ${fields} "${where}" ${tmpDataPath}


if [[ $? -eq 0 ]]
then
echo "执行:  cat ${tmpDataPath}/${game}/${mysqlTable}/data/*.data > ${tmpDataPath}/${game}/${hiveTable}.txt "
cat ${tmpDataPath}/${game}/${mysqlTable}/data/*.data > ${tmpDataPath}/${game}/${hiveTable}.txt
else
exit 1
fi

if [[ $? -eq 0 ]]
then
echo "执行:   hive -e load data local inpath '${tmpDataPath}/${game}/${hiveTable}.txt' overwrite into table ${hiveDatabase}.${tmphiveTable};"
hive -e "load data local inpath '${tmpDataPath}/${game}/${hiveTable}.txt' overwrite into table ${hiveDatabase}.${tmphiveTable};"
else
exit 1
fi


if [[ $? -eq 0 ]]
then
echo "执行:   insert overwrite table ${hiveDatabase}.${hiveTable} partition(date)
select area_id,plat,time,device_id,platform_id,platform_account,server_id,player_id,account,role_id,role_name,recharge_from,money,diamond,item_id,item_num,order_no,cp_order_no,vip_level_pre,role_level_pre,ip,vPaymentGateway,vPayingTypeID,iGrossRevenue_User,iGrossRevenue,iTopupCoin,iCashCoin,iBonusCoin,iTopupCoinAfter,iCashCoinAfter,iBonusCoinAfter,vDescription,iResult,iLevel,iIsFirstPaying,times,online_long_total,tmp1,tmp2,tmp3,tmp4,  to_date(time) as date from ${hiveDatabase}.${tmphiveTable};"

hive -e "insert overwrite table ${hiveDatabase}.${hiveTable} partition(date)
select area_id,plat,time,device_id,platform_id,platform_account,server_id,player_id,account,role_id,role_name,recharge_from,money,diamond,item_id,item_num,order_no,cp_order_no,vip_level_pre,role_level_pre,ip,vPaymentGateway,vPayingTypeID,iGrossRevenue_User,iGrossRevenue,iTopupCoin,iCashCoin,iBonusCoin,iTopupCoinAfter,iCashCoinAfter,iBonusCoinAfter,vDescription,iResult,iLevel,iIsFirstPaying,times,online_long_total,tmp1,tmp2,tmp3,tmp4,  to_date(time) as date from ${hiveDatabase}.${tmphiveTable};"
else
exit 1
fi


if [[ $? -eq 0 ]]
then
echo "执行:  rm -rf ${tmpDataPath}/${game}/${mysqlTable} "
rm -rf ${tmpDataPath}/${game}/${mysqlTable}

echo "执行:  rm ${tmpDataPath}/${game}/${hiveTable}.txt "
rm ${tmpDataPath}/${game}/${hiveTable}.txt
else
exit 1
fi


