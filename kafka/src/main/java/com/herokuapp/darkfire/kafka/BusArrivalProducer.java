package com.herokuapp.darkfire.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import com.herokuapp.darkfire.kafka.utils.BusStopConsts;
import com.herokuapp.darkfire.kafka.utils.Config;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class BusArrivalProducer {


	private static final Logger LOGGER = Logger.getLogger( BusArrivalProducer.class.getName() );

	private final String url = "http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2";

	private final List<String> busStops = new ArrayList<>(Arrays.asList(BusStopConsts.codes));

	public static void main( String[] args )
	{
		LOGGER.info("Checking if API env variable exists");

		if (System.getenv("LTA_DATAMALL") == null){
			LOGGER.log(Level.SEVERE, "No env variable found, exiting");
			return;
		}

		LOGGER.info("env variable exists, starting producer");

		LOGGER.log( Level.INFO, "Starting Producer" );

		new BusArrivalProducer().start();

	}

	private void start() {


		while(true) {

			long currentTimeMillis = System.currentTimeMillis();

			LOGGER.info("making call to api at " + currentTimeMillis);

			List<JSONObject> allResponses = new ArrayList<>();
			
			for (String code : busStops) {

				JSONObject response = getData(code);

				if (response == null) {
					LOGGER.log(Level.SEVERE, "No response received from getData for code: " + code);
				}

				JSONArray services = response.getJSONArray("Services");

				for (int i = 0; i < services.length(); i++) {
					JSONObject service = services.getJSONObject(i);

					JSONObject bus1 = service.getJSONObject("NextBus");
					JSONObject bus2 = service.getJSONObject("NextBus2");
					JSONObject bus3 = service.getJSONObject("NextBus3");

					JSONObject obj = new JSONObject();

					obj.put("BusStopCode", code);
					obj.put("ServiceNo", service.get("ServiceNo"));

					obj.put("NextBusArrival1", bus1.getString("EstimatedArrival").replace("T", " ").replace("+08:00",""));
					obj.put("NextBusArrival2", bus2.getString("EstimatedArrival").replace("T", " ").replace("+08:00",""));
					obj.put("NextBusArrival3", bus3.getString("EstimatedArrival").replace("T", " ").replace("+08:00",""));


					JSONObject[] buses = {bus1, bus2, bus3};
					for (int j = 0; j < buses.length; j++) {
						JSONObject bus = buses[j];

						obj.put("Bus_"+(j+1)+"_DestinationCode", bus.get("DestinationCode"));
						obj.put("Bus_"+(j+1)+"_OriginCode", bus.get("OriginCode"));
						obj.put("Bus_"+(j+1)+"_Latitude", bus.get("Latitude"));
						obj.put("Bus_"+(j+1)+"_Longitude", bus.get("Longitude"));
						obj.put("Bus_"+(j+1)+"_Load", bus.get("Load"));
						obj.put("Bus_"+(j+1)+"_Type", bus.get("Type"));
					}
					

					obj.put("time", System.currentTimeMillis());
					
					allResponses.add(obj);

				}
			}
			
			System.out.println(allResponses.size());

			Properties props = Config.getDefaultProducer();

			Producer<String, String> producer = new KafkaProducer<>(props);

			LOGGER.info("writing to topic busarrival");

			for (int i = 0; i < allResponses.size(); i++) {
				JSONObject obj = allResponses.get(i);
				String key = currentTimeMillis + obj.getString("BusStopCode");
				producer.send(new ProducerRecord<String, String>("busarrival", key , obj.toString()));
			}


			producer.close();


			try {
				Thread.sleep(2 * 60 * 1000);
			} catch (InterruptedException e) {
				LOGGER.log(Level.SEVERE, "Thread interupted.", e);
			}

		}


	}

	private JSONObject getData(final String stop) {

		String apiKey = System.getenv("LTA_DATAMALL");

		OkHttpClient client = new OkHttpClient();

		JSONObject respObj = null;

		Request request = new Request.Builder()
				.url(url+"?BusStopCode="+stop)
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
