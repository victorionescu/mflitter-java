package mflitter.db;

import java.sql.*;

import mflitter.Configuration;

public class Database {
  private final Connection dbConnection;

  public Database() {
    try {
      dbConnection = DriverManager.getConnection(
          "jdbc:mysql://" + Configuration.DB_HOSTNAME + "/" + Configuration.DB_NAME +
          "?user=" + Configuration.DB_USERNAME + "&password=" + Configuration.DB_PASSWORD);
    } catch (SQLException e) {
      throw new IllegalStateException();
    }
  }

  public ResultSet selectQuery(String selectQuery) {
    Statement selectStatement;
    ResultSet selectResults = null;

    try {
      selectStatement = dbConnection.createStatement();

      System.out.println(selectQuery);

      selectResults = selectStatement.executeQuery(selectQuery);
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    }

    return selectResults;
  }

  public void updateQuery(String updateQuery) {
    Statement deleteStatement = null;

    try {
      deleteStatement = dbConnection.createStatement();

      System.out.println(updateQuery);

      deleteStatement.executeUpdate(updateQuery);
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    }
  }
}
