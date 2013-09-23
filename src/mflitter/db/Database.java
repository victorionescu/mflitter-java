package mflitter.db;

import java.sql.*;

public class Database {
  private final Connection dbConnection;

  public Database() {
    try {
      dbConnection = DriverManager.getConnection(
          "jdbc:mysql://127.0.0.1/mflitter?user=root&password=my38008s_52");
    } catch (SQLException e) {
      throw new IllegalStateException();
    }
  }

  public ResultSet selectQuery(String selectQuery) {
    Statement selectStatement = null;
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

  public <E> boolean insertIfNotExistsQuery(String tableName, String insertQuery, String columnToCheck,
      E valueToCheck) {
    Statement insertStatement = null;
    Statement selectStatement = null;
    ResultSet selectResults = null;

    try {
      String selectQuery = "SELECT * FROM `" + tableName + "` WHERE `" + columnToCheck + "`='" +
          valueToCheck.toString() + "'";
      selectStatement = dbConnection.createStatement();
      selectResults = selectStatement.executeQuery(selectQuery);

      if (selectResults.next()) {
        return false;
      }
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    } finally {
      if (selectResults != null) {
        try {
          selectResults.close();
        } catch (SQLException e) { }
      }

      if (selectStatement != null) {
        try {
          selectStatement.close();
        } catch (SQLException e) { }
      }
    }

    try {
      insertStatement = dbConnection.createStatement();
      insertStatement.executeUpdate(insertQuery);

      return true;
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    } finally {
      if (insertStatement != null) {
        try {
          insertStatement.close();
        } catch (SQLException e) { }
      }
    }

    return false;
  }
}
