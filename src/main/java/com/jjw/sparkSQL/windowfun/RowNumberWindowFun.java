package com.jjw.sparkSQL.windowfun;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * row_number()开窗函数：
 * 主要是按照某个字段分组，然后取另一字段的前几个的值，相当于 分组取topN
 * row_number() over (partition by xxx order by xxx desc) xxx
 * 注意：
 * 如果SQL语句里面使用到了开窗函数，那么这个SQL语句必须使用HiveContext来执行，HiveContext默认情况下在本地无法创建
 * @author root
 *
 */
public class RowNumberWindowFun {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("windowfun");
		conf.set("spark.sql.shuffle.partitions","1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sc);
		hiveContext.sql("use spark");
		hiveContext.sql("drop table if exists sales");
		hiveContext.sql("create table if not exists sales (riqi string,leibie string,jine Int) "
				+ "row format delimited fields terminated by '\t'");
		hiveContext.sql("load data local inpath '/root/test/sales' into table sales");
		/**
		 * 开窗函数格式：
		 * 【 row_number() over (partition by XXX order by XXX DESC) as rank】
		 * 注意：rank 从1开始
		 */
		/**
		 * 以类别分组，按每种类别金额降序排序，显示 【日期，种类，金额】 结果，如：
		 * 
		 * 1 A 100
		 * 2 B 200
		 * 3 A 300
		 * 4 B 400
		 * 5 A 500
		 * 6 B 600
		 * 排序后：
		 * 5 A 500  --rank 1
		 * 3 A 300  --rank 2 
		 * 1 A 100  --rank 3
		 * 6 B 600  --rank 1
		 * 4 B 400	--rank 2
		 * 2 B 200  --rank 3
		 * 
		 */
		/*DataFrame result = hiveContext.sql("select riqi,leibie,jine "
							+ "from ("
								+ "select riqi,leibie,jine,"
								+ "row_number() over (partition by leibie order by jine desc) rank "
								+ "from sales) t "
						+ "where t.rank<=3");
		result.show(100);
		*//**
		 * 将结果保存到hive表sales_result
		 *//*
		result.write().mode(SaveMode.Overwrite).saveAsTable("sales_result");*/
		sc.stop();
	}
}
