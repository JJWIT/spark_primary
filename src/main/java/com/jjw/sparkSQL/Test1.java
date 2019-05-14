package com.jjw.sparkSQL;/**
 * Created by jiajianwei1 on 2019/5/14.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @ClassName Test1
 * @Description TODO
 * @Author jiajianwei1
 * @Date 2019/5/14 18:31
 * @Version 1.0
 **/
public class Test1 {
    String jsonPath = "people.json";

    SparkContext sc = null;
    SQLContext sqlContext = null;
    @Before
    public void initContext () {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("test1");
        sc = new SparkContext(conf);
        //创建sqlContext
        sqlContext = new SQLContext(sc);
    }

    /**
     * 读取json文件
     */
    @Test
    public void doReadJson () {
        Dataset<Row> ds = sqlContext.read().format("json").load(jsonPath);
        ds.show();
        ds.printSchema();
        ds.foreach(x -> {
            System.out.println(x);
        });

        ds.createOrReplaceTempView("user"); // 注册成临时表，注意这临时表user只是指向元数据，不会生成真的临时表或者物理表
        Dataset<Row> sql = sqlContext.sql(" select name from user"); // 可以直接使用注册的临时表
        sql.show();
    }

    /**
     * rdd 转为json
     */
    @Test
    public void doReadRdd () {
        /*JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":\"18\"}",
                "{\"name\":\"lisi\",\"age\":\"19\"}",
                "{\"name\":\"wangwu\",\"age\":\"20\"}"
        ));
        JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"score\":\"100\"}",
                "{\"name\":\"lisi\",\"score\":\"200\"}",
                "{\"name\":\"wangwu\",\"score\":\"300\"}"
        ));

        Dataset namedf = sqlContext.read().json(nameRDD);
        Dataset scoredf = sqlContext.read().json(scoreRDD);
        namedf.registerTempTable("name");
        scoredf.registerTempTable("score");

        Dataset result = sqlContext.sql("select name.name,name.age,score.score from name,score where name.name = score.name");
        result.show();*/

    }

    @After
    public void doDestory () {
        sc.stop();
    }
}
