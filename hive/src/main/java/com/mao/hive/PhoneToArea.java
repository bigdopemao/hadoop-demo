package com.mao.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * @author bigdope
 * @create 2018-08-05
 **/
// finalname: phonetoarea
//add jar /home/hadoop/jar/hivejar/phonetoarea.jar
//create temporary function getarea as 'com.mao.hive.PhoneToArea';
//create table flow(phone string,upflow int,downflow int)
//row format delimited
//fields terminated by ','
//load data local inpath '/home/hadoop/hiveTestData/flow.data' into table flow;
//注意：以下这个自定义查询没有运行mapreduce程序
//select getarea(phone), upflow, downflow from flow;
public class PhoneToArea extends UDF {

     private static Map<String, String> areaMap = new HashMap<String, String>();

     static {
         areaMap.put("1388", "beijing");
         areaMap.put("1399", "tianjin");
         areaMap.put("1366", "nanjing");
     }

    //一定要用public修饰才能被hive调用
    public String evaluate(String phone) {
        String result = areaMap.get(phone.substring(0, 4)) == null ? (phone + " huoxing") :
                (phone + "  " + areaMap.get(phone.substring(0, 4)));
        return result;
    }

}