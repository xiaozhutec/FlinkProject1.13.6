package com.johngo.streaming.customeSource;

import com.johngo.domain.Person;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Johngo
 * @date 2022/4/8
 */

public class JGRichSourceMysql extends RichParallelSourceFunction<Person> {

    boolean isRunning = true;
    PreparedStatement ps = null;
    Connection conn = null;

    private Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_prj?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
        } catch (Exception e) {
            System.out.println("connect error then exit." + e.getMessage());
        }
        return con;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = getConnection();
        String executeSql = "select * from person";
        ps = conn.prepareStatement(executeSql);
    }

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {

        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            Person person = new Person();
            person.setId(resultSet.getInt("id"));
            person.setName(resultSet.getString("name"));
            person.setAge(resultSet.getInt("age"));
            person.setSex(resultSet.getInt("sex"));
            person.setSite(resultSet.getString("site"));

            ctx.collect(person);
        }


    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
