package com.herokuapp.darkfire.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import com.herokuapp.darkfire.kafka.utils.Config;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class SpeedBandProducer 
{
	private static final Logger LOGGER = Logger.getLogger( SpeedBandProducer.class.getName() );

	private final String url = "http://datamall2.mytransport.sg/ltaodataservice/TrafficSpeedBandsv2";

	public static void main( String[] args )
	{
		LOGGER.info("Checking if API env variable exists");

		if (System.getenv("LTA_DATAMALL") == null){
			LOGGER.log(Level.SEVERE, "No env variable found, exiting");
			return;
		}

		LOGGER.info("env variable exists, starting producer");

		LOGGER.log( Level.INFO, "Starting Producer" );

		new SpeedBandProducer().start();

	}

	private void start() {


		while(true) {

			long currentTimeMillis = System.currentTimeMillis();
			
			LOGGER.info("making call to api at " + currentTimeMillis);
			
			JSONObject response = getData();

			if (response == null) {
				LOGGER.log(Level.SEVERE, "No response received from getData");
			}

			JSONArray speedbands = response.getJSONArray("value");
			
			
			
			Properties props = Config.getDefaultProducer();

			Producer<String, String> producer = new KafkaProducer<>(props);
			
			LOGGER.info("writing to topic speedband");
			
			for (int i = 0; i < speedbands.length(); i++) {
				JSONObject obj = speedbands.getJSONObject(i);
				obj.put("time", currentTimeMillis);
				String key = currentTimeMillis + obj.getString("RoadName");
				producer.send(new ProducerRecord<String, String>("speedband", key , obj.toString()));
			}
			

			producer.close();

			
			try {
				Thread.sleep(5 * 60 * 1000);
			} catch (InterruptedException e) {
				LOGGER.log(Level.SEVERE, "Thread interupted.", e);
			}

		}


	}

	private JSONObject getData() {

		String apiKey = System.getenv("LTA_DATAMALL");

		OkHttpClient client = new OkHttpClient();

		JSONObject respObj = null;

		Request request = new Request.Builder()
				.url(url)
				.get()
				.addHeader("AccountKey", apiKey)
				.addHeader("accept", "application/json")
				.addHeader("Cache-Control", "no-cache")
				.build();

		try {

			LOGGER.info("making call to api");

			Response response = client.newCall(request).execute();
			respObj = new JSONObject(response.body().string());

		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Call to API failed", e);
		}

		return respObj;
	}

}
