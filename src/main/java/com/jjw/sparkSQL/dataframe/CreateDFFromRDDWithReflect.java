package com.jjw.sparkSQL.dataframe;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
/**
 * 通过反射的方式将非json格式的RDD转换成DataFrame
 * 注意：这种方式不推荐使用
 * @author root
 *
 */
public class CreateDFFromRDDWithReflect {
	public static void main(String[] args) {
		/**
		 * 注意：
		 * 1.自定义类要实现序列化接口
		 * 2.自定义类访问级别必须是Public
		 * 3.RDD转成DataFrame会把自定义类中字段的名称按assci码排序
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("RDD");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lineRDD = sc.textFile("sparksql/person.txt");
		JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(String line) throws Exception {
				Person p = new Person();
				p.setId(line.split(",")[0]);
				p.setName(line.split(",")[1]);
				p.setAge(Integer.valueOf(line.split(",")[2]));
				return p;
			}
		});
		/**
		 * 传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
		 * 在底层通过反射的方式获得Person的所有field，结合RDD本身，就生成了DataFrame
		 */
		DataFrame df = sqlContext.createDataFrame(personRDD, Person.class);
		df.show();
		df.printSchema();
		df.registerTempTable("person");
		DataFrame sql = sqlContext.sql("select  name,id,age from person where id = 2");
		sql.show();
		
		/**
		 * 将DataFrame转成JavaRDD
		 * 注意：
		 * 1.可以使用row.getInt(0),row.getString(1)...通过下标获取返回Row类型的数据，但是要注意列顺序问题---不常用
		 * 2.可以使用row.getAs("列名")来获取对应的列值。
		 * 
		 */
		JavaRDD<Row> javaRDD = df.javaRDD();
		JavaRDD<Person> map = javaRDD.map(new Function<Row, Person>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(Row row) throws Exception {
				Person p = new Person();
//				p.setId(row.getString(0));
//				p.setName(row.getString(1));
//				p.setAge(row.getInt(2));
//				
//				p.setId(row.getString(1));
//				p.setName(row.getString(2));
//				p.setAge(row.getInt(0));
//				
				p.setId(row.getAs("id")+"");
				p.setName((String)row.getAs("name"));
				p.setAge((Integer)row.getAs("age"));
				return p;
			}
		});
		map.foreach(new VoidFunction<Person>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Person person) throws Exception {
				System.out.println(person);
			}
		});
		
		sc.stop();
	}
}
