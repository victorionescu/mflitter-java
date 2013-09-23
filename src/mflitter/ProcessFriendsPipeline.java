package mflitter;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import java.util.Locale;

import mflitter.db.TokenIterator;


public class ProcessFriendsPipeline {
  public static void setNextCursor(Connection dbConnection, long jobId, long nextCursor) {
    Statement updateQueryStatement = null;

    try {
      updateQueryStatement = dbConnection.createStatement();
      String updateQuery = "UPDATE `friends_jobs` SET `next_cursor`=" + nextCursor +
          " WHERE `id`=" + jobId;
      System.out.println(updateQuery);
      updateQueryStatement.executeUpdate(updateQuery);
    } catch (SQLException e) {
      System.out.println("SQLException: " + e.getMessage());
      System.out.println("SQLState: " + e.getSQLState());
      System.out.println("VendorError: " + e.getErrorCode());
    } finally {
      if (updateQueryStatement != null) {
        try {
          updateQueryStatement.close();
        } catch (SQLException e) { }
      }
    }
  }

  public static void main(String[] args) {
    for (int step = 0; step < 10; step += 1) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) { }

      Statement queryStatement = null;
      ResultSet resultSet = null;

      try {
        Connection dbConnection = DriverManager.getConnection("jdbc:mysql://localhost/mflitter?" +
            "user=root&password=my38008s_52");

        queryStatement = dbConnection.createStatement();
        resultSet = queryStatement.executeQuery(
            "SELECT `friends_jobs`.*, `users`.`access_token`, `users`.`access_token_secret`" +
                "FROM `friends_jobs`,`users` WHERE `users`.`id`=`friends_jobs`.`user_id` AND " +
                "`friends_jobs`.`next_cursor` != 0 " +
                "ORDER BY `issued_at` DESC LIMIT 1");

        if (resultSet.next()) {
          System.out.println("ASSIGNED JOB: " + resultSet.getString("screen_name") +
              "  NEXT CURSOR: " + resultSet.getLong("next_cursor"));

          System.out.println("ACCESS TOKEN: " + resultSet.getString("access_token"));
          System.out.println("ACCESS TOKEN SECRET: " + resultSet.getString("access_token_secret"));

          long jobId = resultSet.getLong("id");
          long userId = resultSet.getLong("user_id");
          String screenName = resultSet.getString("screen_name");
          String accessToken = resultSet.getString("access_token");
          String accessTokenSecret = resultSet.getString("access_token_secret");
          long nextCursor = resultSet.getLong("next_cursor");


          ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
          configurationBuilder.setOAuthConsumerKey("uGV0PkC1YSaTcf0I4kt0sg");
          configurationBuilder.setOAuthConsumerSecret(
              "cY2IRClYHy4uG08EjOH59BnX59tgibfqDOBLGHZB3CQ");
          configurationBuilder.setOAuthAccessToken(accessToken);
          configurationBuilder.setOAuthAccessTokenSecret(accessTokenSecret);

          TwitterFactory twitterFactory = new TwitterFactory(configurationBuilder.build());
          Twitter twitter = twitterFactory.getInstance();

          try {
            PagableResponseList<User> friends = twitter.getFriendsList(screenName, nextCursor);

            for (User user : friends) {
              Statement insertQueryStatement = null;
              try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String insertQuery =
                    "INSERT INTO friends(screen_name, user_id, created_at, default_profile, " +
                    "default_profile_image, profile_image_url) VALUES('" +
                    user.getScreenName() + "', " + userId + ", '" +
                    dateFormat.format(user.getCreatedAt()) + "', " +
                    user.defaultProfile() + ", " + user.defaultProfileImage() +
                    ", '" + user.getProfileImageURL() + "')";
                System.out.println(insertQuery);
                //throw new SQLException();
                insertQueryStatement = dbConnection.createStatement();
                insertQueryStatement.executeUpdate(insertQuery);
                setNextCursor(dbConnection, jobId, friends.getNextCursor());

              } catch (SQLException e) {
                System.out.println("SQLException: " + e.getMessage());
                System.out.println("SQLState: " + e.getSQLState());
                System.out.println("VendorError: " + e.getErrorCode());
              } finally {
                if (insertQueryStatement != null) {
                  try {
                    insertQueryStatement.close();
                  } catch (SQLException e) { }
                }
              }
            }
          } catch (TwitterException e) {
            System.out.println("Could not fetch results from API.");
          }


        } else {
          System.out.println("No jobs for now.");
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
    }

    /*String TWITTER_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy";
    SimpleDateFormat sf = new SimpleDateFormat(TWITTER_FORMAT);
    sf.setLenient(false);

    try {

      System.out.println(sf.parse("Mon Nov 29 21:18:15 +0000 2010"));
    } catch (ParseException e) {
      System.out.println("nasol cu datele");
    }*/


    //System.out.println("Life is good!");
  }
}
