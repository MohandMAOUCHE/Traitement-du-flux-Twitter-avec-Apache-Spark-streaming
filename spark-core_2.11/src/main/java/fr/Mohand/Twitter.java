package fr.Mohand;

import java.util.List;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter {

	public static void main(String[] args) throws TwitterException {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDaemonEnabled(true);
		
		TwitterFactory tf = new TwitterFactory();
		twitter4j.Twitter twitter = tf.getInstance();
		
		List<Status> status = twitter.getHomeTimeline();
		for(Status st : status) {
			System.out.println(st.getUser().getName()+"-------"+st.getText());
		}

	}
	
}
