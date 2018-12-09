package com.cloudcomputing.samza.ny_cabs;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import org.json.*;

import java.security.Key;
import java.util.*;
import java.io.InputStream;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream 
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /* 
       Define per task state here. (kv stores etc) 
       READ Samza API part in Writeup to understand how to start 
    */
    private Integer MIN_DURATION = 5 * 60 * 1000;
    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getStore("yelp-info");

        //Initialize static data and save them in kv store
        initialize();
    }

    /**
     * This function will read the static data from resources folder 
     * and save data in KV store.
     * 
     * This is just an example, feel free to change them.
     */
    
     public void initialize(){
        ClassLoader classLoader = this.getClass().getClassLoader();

        // Read "UserInfoData.json" from resources folder
        InputStream s = classLoader.getResourceAsStream("UserInfoData.json");
        Scanner scanner = new Scanner(s);
        
        while(scanner.hasNextLine()){
            // read data and put them in KV store
            Map<String, Object> map = new HashMap<>();
            String line = scanner.nextLine();
            JSONObject obj = new JSONObject(line);
            int userId = obj.getInt("userId");
            for(String k : obj.keySet()){
                map.put(k, obj.get(k));
            }
            userInfo.put(userId, map);
        }

        s = classLoader.getResourceAsStream("NYCstore.json");
        scanner = new Scanner(s);
        
        while(scanner.hasNextLine()){
            // read data and put them in KV store
            String line = scanner.nextLine();
            Map<String, Object> map = new HashMap<>();
            JSONObject obj = new JSONObject(line);
            String storeId = obj.getString("storeId");
            String category = obj.getString("categories");
            for(String k : obj.keySet()){
                map.put(k, obj.get(k));
            }
            yelpInfo.put(category + ":" + storeId, map);
        }
        scanner.close();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /**
         * All the messsages are partitioned by blockId, which means the messages
         * sharing the same blockId will arrive at the same task, similar to the
         * approach that MapReduce sends all the key value pairs with the same key
         * into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
	    // Handle Event messages
            processAdMatch((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void processAdMatch(Map<String, Object> event, MessageCollector collector) {
        /**
         * We only care about two types of events here: RIDER_INTEREST, RIDE_REQUEST
         * If it is RIDER_INTEREST, then update the current interest of the user if necessary
         * If it is RIDE_REQUEST, then we pick the most matched store and send the output stream
         */
         String type  = (String) event.get("type");
         if (type.equals("RIDER_INTEREST")) {
             Integer duration = (Integer) event.get("duration");
             // Only update if the duration is greater than 5 minutes.
             if (duration <= MIN_DURATION) return;
             Integer userId = (Integer) event.get("userId");
             String interest = (String) event.get("interest");
             // Get or default
             Map<String, Object> user = userInfo.get(userId);
             if (user == null) user = new HashMap<>();
             // Update the current interest
             user.put("interest", interest);
             userInfo.put(userId, user);
         } else if (type.equals("RIDE_REQUEST")) {
             computeScore(event, collector);
         }

    }

    private double distance(double lat1, double lat2, double lon1, double lon2) {
        /**
         * Compute the distance between two coordinates in the unit of mile
         */
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0.0;
        }
        else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515; // in mile
            return dist;
        }
    }

    private int computePriceValue(String price) {
        /**
         * Convert the string representation of price to integer
         */
         int priceValue = 0;
         switch (price) {
             case "$":
                 priceValue = 1;
                 break;
             case "$$":
                 priceValue = 2;
                 break;
             case "$$$":
                 priceValue = 3;
                 break;
             case "$$$$":
                 priceValue = 3;
                 break;
             default:
                 priceValue = 0;
         }
         return priceValue;
    }

    private int computeDeviceValue(String device) {
        /**
         * Convert the string representation of device to integer
         */
         int deviceValue = 0;
         switch (device) {
             case "iPhone XS":
                 deviceValue = 3;
                 break;
             case "iPhone 7":
                 deviceValue = 2;
                 break;
             case "iPhone 5":
                 deviceValue = 1;
                 break;
             default:
                 deviceValue = 0;
         }
         return deviceValue;
    }

    private void computeScore(Map<String, Object> event, MessageCollector collector) {
        /**
         * We find the interest of the user and search for all stores which has the interest
         * For all potential store, we compute the match score and select the most matched one.
         */
        // Extract the user information
        Integer userId = (Integer) event.get("clientId");
        Double latitude = (Double) event.get("latitude");
        Double longitude = (Double) event.get("longitude");
        Map<String, Object> user = userInfo.get(userId);
        String device = (String) user.get("device");
        Integer deviceValue = computeDeviceValue(device);
        String interest = (String) user.get("interest");
        Integer travelCount = (Integer) user.get("travel_count");
        Integer age = (Integer) user.get("age");
        // Search for the store that match the user's current interest.
        KeyValueIterator<String, Map<String, Object>> candidates = yelpInfo.range(interest + ":", interest + ";");
        Double maxMatchScore = 0.0;
        String maxStoreId = "";
        String maxStoreName = "";
        // Compute the match score for each store candidate
        while (candidates.hasNext()) {
            try {
                Entry<String, Map<String, Object>> cand = candidates.next();
                Map<String, Object> store =  cand.getValue();
                // Extract store information
                Integer reviewCount = (Integer) store.get("review_count");
                Double rating = (Double) store.get("rating");
                String price = (String) store.get("price");
                Integer priceValue = computePriceValue(price);
                Double sLatitude = (Double) store.get("latitude");
                Double sLongitude = (Double) store.get("longitude");
                String name = (String) store.get("name");
                String storeId = (String) store.get("storeId");
                // Compute the match score
                Double matchScore = reviewCount * rating;
                matchScore = matchScore * (1 - Math.abs(priceValue - deviceValue) * 0.1);
                Double distance = distance(latitude, sLatitude, longitude, sLongitude);
                if (travelCount > 50 || age == 20) {
                    // Young users or those who travel a lot
                    if (distance > 10) {
                        matchScore = matchScore * 0.1;
                    }
                } else {
                    // Other users.
                    if (distance > 5) {
                        matchScore = matchScore * 0.1;
                    }
                }
                // Update the max match score.
                if (matchScore > maxMatchScore) {
                    maxMatchScore = matchScore;
                    maxStoreId = storeId;
                    maxStoreName = name;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        candidates.close();
        // Send the driver-client pair information
        HashMap<String, Object> result = new HashMap<>();
        result.put("userId", userId);
        result.put("storeId", maxStoreId);
        result.put("name", maxStoreName);
        collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, result));
    }
}
