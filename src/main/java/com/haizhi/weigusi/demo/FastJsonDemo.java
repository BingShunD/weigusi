package com.haizhi.weigusi.demo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastJsonDemo {
    public static void main(String[] args) {
//        JSONObject jo = new JSONObject();
//        String taskIds = "[\"task_aaa\",\"task_bbb\"]";
//        jo.put("taskIds", taskIds);
//        System.out.println(jo);
//        System.out.println("===========");
//        System.out.println(jo.toString());


        String ss= "{\n" +
                "            \"aaa\":\"bbb\",\n" +
                "                \"ids\": [\"asdasd\",\"asdasd\"]\n" +
                "        }";

        JSONObject jo = JSONObject.parseObject(ss);

        System.out.println(jo.getString("aaa"));

        System.out.println("=========");
        JSONArray ids = jo.getJSONArray("ids");
        System.out.println(ids.toString());


    }
}
