package com.jjw.sparkCore.others;

import com.jjw.sparkCore.bean.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromWithReflect {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("RDD");

        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
        JavaRDD<String> stringJavaRDD = context.textFile("./person.json");
        JavaRDD<Person> personJavaRDD1 = stringJavaRDD.map(new Function<String, Person>() {

            /**
             * TODO 如果在这里实例化Person，即在executor中实例化，相当于直接把代码逻辑传输到executor端，这时候Person不需要序列化，
             *  也就不需要实现Serializable接口
             */
            Person person = new Person();

            @Override
            public Person call(String row) throws Exception {
                System.out.println("row = " + row);
                person.setAge(row.split(",")[0]);
                person.setName(row.split(",")[1]);
                return person;
            }
        });


        /**
         * 放在外面就是在driver端执行，Person对象需要序列化到对应的executor端，并在excutor端反序列化
         */
        Person person = new Person();
        JavaRDD<Person> personJavaRDD2 = stringJavaRDD.map(new Function<String, Person>() {

            @Override
            public Person call(String row) throws Exception {
                person.setAge(row.split(",")[0]);
                person.setName(row.split(",")[1]);
                return person;
            }
        });

        Dataset<Row> dataFrame = sqlContext.createDataFrame(personJavaRDD1, Person.class);
        dataFrame.printSchema();
        dataFrame.show();
        context.stop();
    }

}
