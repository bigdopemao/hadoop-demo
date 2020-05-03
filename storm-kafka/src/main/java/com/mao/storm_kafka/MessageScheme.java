package com.mao.storm_kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author bigdope
 * @create 2018-09-03
 **/
public class MessageScheme implements Scheme {

    private static final long serialVersionUID = 8423372426211017613L;

    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes, "UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Fields getOutputFields() {
        return new Fields("msg");
    }

}
