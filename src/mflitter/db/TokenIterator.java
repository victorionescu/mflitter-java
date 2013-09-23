package mflitter.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import twitter4j.auth.AccessToken;

/**
 * Class that cycles through the access tokens table.
 * Policy is set to LRU (least recently used).
 *
 * @author victorionescu46@gmail.com (Victor Ionescu)
 */
public class TokenIterator {
  private final Connection dbConnection;
  private final String operationType;

  public TokenIterator(Connection dbConnection, String operationType) {
    this.dbConnection = dbConnection;
    this.operationType = operationType;
  }

  public AccessToken nextToken() {
    Statement queryStatement = null;
    ResultSet resultSet = null;
    try {
      queryStatement = dbConnection.createStatement();
      resultSet = queryStatement.executeQuery("SELECT * FROM `users` WHERE `" +
          operationType + "_timestamp` IS NULL LIMIT 1");
      if (resultSet.next()) {
        String accessToken = resultSet.getString("access_token");
        String accessTokenSecret = resultSet.getString("access_token_secret");
        long userId = resultSet.getLong("user_id");

        return new AccessToken(accessToken, accessTokenSecret, userId);
      }
      resultSet = queryStatement.executeQuery("SELECT * FROM `users` ORDER BY `" +
          operationType + "_timestamp` ASC LIMIT 1");

      if (resultSet.next()) {
        String accessToken = resultSet.getString("access_token");
        String accessTokenSecret = resultSet.getString("access_token_secret");
        long userId = resultSet.getLong("user_id");

        return new AccessToken(accessToken, accessTokenSecret, userId);
      }
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) { }
      }

      if (queryStatement != null) {
        try {
          queryStatement.close();
        } catch (SQLException e) { }
      }
    }

    return null;
  }
}
