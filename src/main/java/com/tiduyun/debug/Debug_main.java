package com.tiduyun.debug;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author hzj
 * @date 2021/5/7 1:28
 */
public class Debug_main {

    public static String mysql_create_sql="create table sinkTable ( debug_field varchar(100));";

    public static String mysql_url="";
    public static String mysql_table_name="sinkTable";
    public static String mysql_username="";
    public static String mysql_password="";
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new Debug_source());

        tEnv.createTemporaryView("sourceTable",stringDataStreamSource,$("debug_field"));

        tEnv.executeSql(
                "CREATE TABLE sinkTable (\n" +
                "  debug_field STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = '"+mysql_url+"'," + "\n" +
                "   'table-name' = '"+mysql_table_name+"'," + "\n" +
                "   'username' = '"+mysql_username+"'," + "\n" +
                "   'password' = '"+mysql_password+"'" + "\n" +

                "   ," +
                        //
                "   'sink.buffer-flush.max-rows'= '0', "+
                "   'sink.buffer-flush.interval'= '0' "+


                ")");

        tEnv.executeSql("INSERT INTO sinkTable SELECT debug_field FROM sourceTable");


    }
}
