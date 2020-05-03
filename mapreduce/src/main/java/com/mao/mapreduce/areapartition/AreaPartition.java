package com.mao.mapreduce.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author bigdope
 * @create 2018-07-12
 **/
public class AreaPartition<Key, Value> extends Partitioner<Key, Value> {

    private static Map<String, Integer> areaMap = new HashMap<String, Integer>();

    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }

    @Override
    public int getPartition(Key key, Value value, int i) {

        //从key中拿到手机号，查询手机归属地字典，不同的省份返回不同的组号
        int areaCoder = areaMap.get(key.toString().substring(0, 3)) == null ? 5 : areaMap.get(key.toString().substring(0, 3));

        return areaCoder;
    }

}
