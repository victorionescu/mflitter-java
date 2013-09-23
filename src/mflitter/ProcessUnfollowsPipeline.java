package mflitter;

import mflitter.db.Database;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ProcessUnfollowsPipeline {
  public static void main(String[] args) throws SQLException {
    Database database = new Database();

    ResultSet resultSet = database.selectQuery(
        "SELECT * FROM `unfollows` LIMIT 1");

    if (resultSet != null && resultSet.next()) {
      System.out.println("Found user in unfollow queue.");

      long userId = resultSet.getLong("user_id");

      ResultSet friendsSet = database.selectQuery(
          "SELECT `unfollows_friends`.*, `users`.`access_token`, `users`.`access_token_secret` " +
          "FROM `unfollows_friends`, `users` WHERE `unfollows_friends`.`user_id`=`users`.`id` " +
          "AND `unfollows_friends`.`status`=false");

      boolean apiException = false;

      while (friendsSet.next()) {
        System.out.println("FRIEND " + friendsSet.getString("screen_name"));

        String friendScreenName = friendsSet.getString("screen_name");

        String accessToken = friendsSet.getString("access_token");
        String accessTokenSecret = friendsSet.getString("access_token_secret");

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey("uGV0PkC1YSaTcf0I4kt0sg");
        configurationBuilder.setOAuthConsumerSecret(
            "cY2IRClYHy4uG08EjOH59BnX59tgibfqDOBLGHZB3CQ");
        configurationBuilder.setOAuthAccessToken(accessToken);
        configurationBuilder.setOAuthAccessTokenSecret(accessTokenSecret);

        TwitterFactory twitterFactory = new TwitterFactory(configurationBuilder.build());
        Twitter twitter = twitterFactory.getInstance();

        try {
          twitter.destroyFriendship(friendScreenName);
          database.updateQuery("UPDATE `unfollows_friends` SET `status`=true WHERE `user_id`=" +
              userId + " AND `screen_name`='" + friendScreenName + "'");
        } catch (TwitterException e) {
          System.out.println("ERROR communicating with the Twitter API");
          apiException = true;
        }
      }

      if (!apiException) {
        database.updateQuery("DELETE FROM `unfollows` WHERE `user_id`=" + userId);
      }

      database.updateQuery("DELETE FROM `friends` WHERE `user_id`=" + userId);
      database.updateQuery("UPDATE `friends_jobs` SET `next_cursor`=-1 WHERE `user_id`=" + userId);
    } else {
     System.out.println("No jobs to process.");
    }

    System.out.println("Pipeline STOP.");
  }
}
