package com.jjw.sparkCore.pipeline;/**
 * Created by jiajianwei1 on 2019/9/27.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName AudienceMappingStep
 * @Description TODO
 * @Author jiajianwei1
 * @Date 2019/9/27 20:07
 * @Version 1.0
 **/
public class AudienceMappingStep {

    private static Logger logger = LoggerFactory.getLogger(AudienceMappingStep.class);
    /**
     * Method Name main
     * @param args
     * Return Type void
     */
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local").setAppName("pipeline");

        final int deviceType = 48;
        long audienceSize = 0;

        SparkSession sparkSession = SparkSession.builder()
                .enableHiveSupport()
                .getOrCreate();

        // 模拟数据
//        sparkSession.read.json("examples/src/main/resources/people.json");

        // 模拟pipeline数据
        Dataset<Row> mappingDataset = sparkSession.read().table("app.app_m14_y_busdep_mac_pin ")
                .filter("mac is not null").dropDuplicates("mac", "user_log_acct")
                .selectExpr("upper(mac) as pin1", "user_log_acct as pin2")
                .repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // 人群包数据
        Dataset<Row> rowDatasetSource = null;
        //数据关联hive表后,匹配上pin的设备号
        Dataset<Row> matchingPinDataset = mappingDataset.filter("pin2 is not null").join(rowDatasetSource.selectExpr("upper(pin1) as pin1"), "pin1").select("pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        logger.info("device2Pin matchingPinDataset:{}", matchingPinDataset.count());

        //数据关联hive表后,未匹配上pin的设备号
//        Dataset<Row> mismatchingPinDataset = mappingDataset.filter("pin2 is null").selectExpr("concat('d:" + deviceType + ":', pin1) as pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        Dataset<Row> mismatchingPinDataset = rowDatasetSource.except(mappingDataset.select("pin1")).selectExpr("concat('d:" + deviceType + ":', pin1) as pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        logger.info("device2Pin mismatchingPinDataset:{}", mismatchingPinDataset.count());

        //合并匹配上pin和未匹配上pin的设备数据
        final Dataset<Row> rowDataset = matchingPinDataset.union(mismatchingPinDataset).sortWithinPartitions("pin2").dropDuplicates("pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        //通过spark写入JFS
        audienceSize = rowDataset.count();
        System.out.println(audienceSize);
    }
}
