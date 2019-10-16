package com.flinkinpratice.chapter11;

import com.alibaba.fastjson.JSONObject;
import com.flinkinpratice.chapter11.utils.DBConnectUtil2;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * kafka to mysql two phase commit, extends TwoPhaseCommitSinkFunction
 * https://www.jianshu.com/p/5bdd9a0d7d02
 *
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode,Connection,Void> {

    private static final Logger log = LoggerFactory.getLogger(MySqlTwoPhaseCommitSink.class);

    public MySqlTwoPhaseCommitSink(){
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * prepare insert statement
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        log.info("start invoke...");
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        log.info("===>date:" + date + " " + objectNode);
        log.info("===>date:{} --{}",date,objectNode);
        String value = objectNode.get("value").toString();
        log.info("objectNode-value:" + value);
        JSONObject valueJson = JSONObject.parseObject(value);
        String value_str = (String) valueJson.get("value");
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,value_str);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(2,value_time);
        log.info("To be inserted:{}--{}",value_str,value_time);

        ps.execute();

        // simulate an exception, comment out these 3 lines if you want to continue to run
        if(Integer.parseInt(value_str) == 15) {
            System.out.println(1 / 0);
        }
    }

    /**
     * Begin transaction
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil2.getConnection(url, "root", "example");
        return connection;
    }

    /**
     *
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * If `invoke` method execute correctly,then do real commit
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil2.commit(connection);
    }

    /**
     * If there is an exception, then rollback; also skip the next checkpoint operation
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil2.rollback(connection);
    }
}