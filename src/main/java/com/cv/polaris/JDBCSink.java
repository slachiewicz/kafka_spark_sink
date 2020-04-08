package com.cv.polaris;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;

import static com.nrt.test.constants.Constants.*;
import static com.nrt.test.utils.PropertiesConfig.getProp;


public class JDBCSink extends org.apache.spark.sql.ForeachWriter<Row> implements Serializable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JDBCSink.class);
    private static Connection connection;
    private static PreparedStatement statement;
    private static PreparedStatement statement2;
    private static PreparedStatement stmt;
    private static Logger log = Logger.getLogger(JDBCSink.class);

    @Override
    public boolean open(long partitionId, long epochId) {
        try {
            log.info("JDBC sink: In open method: " + new Timestamp(System.currentTimeMillis()));
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(getProp().getProperty("ora_host"),
                    getProp().getProperty("user_name"),
                    getProp().getProperty("user_passwd"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            LOGGER.info("forName Error....");
            log.info("JDBC sink: (class not found exception) Error occurred in open method: " + new Timestamp(System
                    .currentTimeMillis()), e);
        } catch (SQLException e) {
            e.printStackTrace();
            log.info("JDBC sink: (SQL exception) Error occurred in open method: " + new Timestamp(System
                    .currentTimeMillis()), e);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("JDBC sink: (IO exception) Error occurred in open method: " + new Timestamp(System
                    .currentTimeMillis()), e);
        }
        LOGGER.info("Oracle Target connection :: " + System.nanoTime() + " ", connection);

        if (connection != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void process(Row value) {

        try {
            log.info("JDBC sink: In process method: " + new Timestamp(System.currentTimeMillis()));
            String sqlQuery2 = INSERT_INTO_NRT_PAYMENT_EVENT_DELTA;

            statement = connection.prepareStatement(sqlQuery2);
            LOGGER.info("Oracel Target connection in process :: " + System.nanoTime() + " ", connection);
            stmt = JDBCSinkOps.addParamsToStatement(statement, value);

            stmt.execute();
            LOGGER.info("Insert statement for intermediate table result= ");
            stmt.close();
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
            log.error("JDBC sink: Error occurred in process method: " + new Timestamp(System.currentTimeMillis()), e);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        LOGGER.info("Oracle Target connection in close ::" + System.nanoTime() + " ", connection);
        log.info("JDBC sink: In close method: " + new Timestamp(System.currentTimeMillis()));
        if (connection != null) {

            String sqlQuery21 = MERGE_INTO_FINAL_TABLE;
            int i = 0;
            try {
                statement2 = connection.prepareStatement(sqlQuery21);
                i = statement2.executeUpdate(sqlQuery21);
                LOGGER.info("Merge statement result= " + i);
                statement2.close();
                if (i > 0) {
                    String sqlQuery3 = "TRUNCATE TABLE nrt_payment_event_delta";
                    statement2 = connection.prepareStatement(sqlQuery3);
                    statement2.execute(sqlQuery3);
                    statement2.close();
                    LOGGER.info("TRUNCATE TABLE nrt_payment_event_delta");
                }

            } catch (SQLException e) {
                e.printStackTrace();
                log.error("JDBC sink: In close method: " + new Timestamp(System.currentTimeMillis()), e);
            }

            try {
                connection.close();

            } catch (SQLException e) {
                e.printStackTrace();
                log.error("JDBC sink: In close method: " + new Timestamp(System.currentTimeMillis()), e);
            }
        }
        LOGGER.info("Oracle Target connection after close:: " + System.nanoTime() + " ", connection);
    }
}