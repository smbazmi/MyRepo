package com.ukg.peoplefabric.bqtodatastore.util;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class JDBCResultSetSharingExample {
  public static void main(String[] args)
      throws SQLException, ClassNotFoundException, InterruptedException {
    Class.forName("org.postgresql.Driver");
    String jdbcUrl = "jdbc:postgresql://10.251.56.83:5432/test_syed";
    String dbUser = "postgres";
    String dbPassword = "-TZHg4NRCjoGzuSP";

    int numThreads = 1;

    for (int i = 1; i <= numThreads; i++) {

      int l = i;
      Connection connection =
          DriverManager.getConnection(jdbcUrl, BqToCloudSqlUtils.getProperties(dbUser, dbPassword));
      ;
      Thread thread =
          new Thread(
              () -> {
                try {

                  String[] timestamps = {
                    "1706918400.0", "2024-02-02T00:20:00.123456",
                  };
                  String query =
                      "INSERT INTO people_fabric.\"syed\" (\"partitionDate\", \"tmpDate\", \"timetest\") VALUES(?,?,?)";
                  try (PreparedStatement statement = connection.prepareStatement(query)) {
                    for (String timestamp : timestamps) {

                      if (BqToCloudSqlUtils.isUnixTimestamp(timestamp)) {
                        Instant instant = BqToCloudSqlUtils.parseUnixTimestamp(timestamp);
                        BigDecimal unixTimestamp = new BigDecimal(timestamp);
                        long nanos1 =
                            unixTimestamp.multiply(BigDecimal.valueOf(1_000_000_000)).longValue();
                        DateTimeFormatter formatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'");
                        long j = nanos1 % 1000000000;
                        LocalDateTime localDateTime =
                            LocalDateTime.ofEpochSecond(
                                nanos1 / 1_000_000_000, (int) j, ZoneOffset.UTC);
                        LocalDateTime localDateTime2 =
                            LocalDateTime.parse(
                                localDateTime.format(formatter),
                                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"));
                        statement.setTimestamp(1, Timestamp.from(instant));
                      } else {
                        LocalDateTime formattedTimestamp =
                            LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
                        System.out.println(formattedTimestamp);
                        statement.setTimestamp(2, java.sql.Timestamp.valueOf(formattedTimestamp));
                      }
                    }
                    LocalTime localTime = LocalTime.parse("07:53:11.477577");
                    System.out.println(Time.valueOf(localTime));
                    statement.setObject(3, localTime);
                    statement.addBatch();
                    statement.executeBatch();
                    connection.close();
                  }
                } catch (SQLException e) {
                  System.out.println(e);
                  e.printStackTrace();
                }
              });

      thread.start();
    }
  }
}
