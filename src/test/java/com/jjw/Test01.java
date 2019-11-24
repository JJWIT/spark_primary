package com.jjw;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.regex.Pattern;

import static org.apache.spark.sql.RowFactory.*;

public class Test01 {

    private static final Pattern phonePattern = Pattern.compile("^1[3-9]\\d{9}$");

    private static final Pattern chinesePattern = Pattern.compile("[\u4e00-\u9fa5]");

    public static void main(String[] args) {

        String msg = "112@qq.cm";

        System.out.println(chinesePattern.matcher(msg).find());
    }
}
