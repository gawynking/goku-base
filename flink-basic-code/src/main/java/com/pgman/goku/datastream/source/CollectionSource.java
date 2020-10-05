package com.pgman.goku.datastream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionSource {


    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        // 1 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 注册数据源
        // StreamExecutionEnvironment.fromCollection() 可以用来加载集合类型数据源
        DataStreamSource<people> stream = env.fromCollection(Arrays.asList(
                new people("pgman", 30, "男"),
                new people("chavin", 30, "男"),
                new people("nope", 28, "女")
        ));

        stream.print();

        env.execute("CollectionSource");

    }


    public static class people{

        String name;
        Integer age;
        String sex;

        public people(String name, Integer age, String sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "people{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex='" + sex + '\'' +
                    '}';
        }
    }

}
