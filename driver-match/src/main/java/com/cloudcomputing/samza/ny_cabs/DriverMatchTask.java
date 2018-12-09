package com.cloudcomputing.samza.ny_cabs;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc) 
       READ Samza API part in Writeup to understand how to start 
    */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, Map<String, String>> drivers;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        drivers = (KeyValueStore<String, Map<String, String>>) context.getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
	    // Handle Driver Location messages
            processDriverLocation((Map<String, Object>) envelope.getMessage());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
	    // Handle Event messages
            processEvent((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void processDriverLocation(Map<String, Object> driverLocation) {
        Integer blockId = (Integer) driverLocation.get("blockId");
        Integer driverId = (Integer) driverLocation.get("driverId");
        Double latitude = (Double) driverLocation.get("latitude");
        Double longitude = (Double) driverLocation.get("longitude");
        String key = blockId.toString() + ":" + driverId.toString();
        Map<String, String> driver = drivers.get(key);
        if (driver == null) driver = new HashMap<>();
        driver.put("latitude", latitude.toString());
        driver.put("longitude", longitude.toString());
        drivers.put(key, driver);
    }

    private void processEvent(Map<String, Object> event, MessageCollector collector) {
        Integer blockId = (Integer) event.get("blockId");
        String type = (String) event.get("type");
        if (type.equals("RIDE_REQUEST")) {
            Integer clientId = (Integer) event.get("clientId");
            Double latitude = (Double) event.get("latitude");
            Double longitude = (Double) event.get("longitude");
            String genderPreference = (String) event.get("gender_preference");
            // Search for all drivers with the same blockId
            KeyValueIterator<String, Map<String, String>> candidates = drivers.range(blockId.toString() + ":", blockId.toString() + ";");
            Integer matchedId = -1;
            Double maxMatchScore = 0.0;
            while (candidates.hasNext()) {
                try {
                    Entry<String, Map<String, String>> cand = candidates.next();
                    Map<String, String> driver = cand.getValue();
                    // Extract driver information from the hashmap
                    Integer driverId = Integer.valueOf(cand.getKey().split(":")[1]);
                    Double dLatitude = Double.valueOf(driver.get("latitude"));
                    Double dLongitude = Double.valueOf(driver.get("longitude"));
                    String dGender = driver.get("gender");
                    String dStatus = driver.get("status");
                    if (dStatus.equals("UNAVAILABLE")) continue;
                    Double dRating = Double.valueOf(driver.get("rating"));
                    Integer dSalary = Integer.valueOf(driver.get("salary"));
                    // Compute all four scores and the total weighted score
                    Double distanceScore = Math.pow(Math.E, -1 * clientDriverDistance(latitude, longitude, dLatitude, dLongitude));
                    Double ratingScore = dRating / 5.0;
                    Double genderScore = (genderPreference.equals(dGender) || genderPreference.equals("N")) ? 1.0 : 0.0;
                    Double salaryScore = 1 - dSalary / MAX_MONEY;
                    // match_score = distance_score * 0.4 + gender_score * 0.1 + rating_score * 0.3 + salary_score * 0.2
                    Double matchScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;
                    if (matchScore > maxMatchScore) {
                        maxMatchScore = matchScore;
                        matchedId = driverId;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            candidates.close();
            drivers.delete(blockId.toString() + ":" + matchedId.toString());
            HashMap<String, Integer> pair = new HashMap<>();
            pair.put("clientId", clientId);
            pair.put("driverId", matchedId);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, pair));

        } else {
            Integer driverId = (Integer) event.get("driverId");
            Double dLatitude = (Double) event.get("latitude");
            Double dLongitude = (Double) event.get("longitude");
            if (type.equals("LEAVING_BLOCK")) {
                drivers.delete(blockId.toString() + ":" + driverId.toString());
            } else if (type.equals("ENTERING_BLOCK")) {
                String dGender = (String) event.get("gender");
                Double dRating = (Double) event.get("rating");
                Integer dSalary = (Integer) event.get("salary");
                String dStatus = (String) event.get("status");
                // Retrieve the driver from the key-value map
                String key = blockId.toString() + ":" + driverId.toString();
                Map<String, String> driver = drivers.get(key);
                if (driver == null) driver = new HashMap<>();
                // Update the driver information
                driver.put("latitude", dLatitude.toString());
                driver.put("longitude", dLongitude.toString());
                driver.put("gender", dGender);
                driver.put("rating", dRating.toString());
                driver.put("salary", dSalary.toString());
                driver.put("status", dStatus.toString());
                drivers.put(key, driver);
            } else if (type.equals("RIDE_COMPLETE")) {
                String dGender = (String) event.get("gender");
                Double dRating = (Double) event.get("rating");
                Integer dSalary = (Integer) event.get("salary");
                Double uRating = (Double) event.get("user_rating");
                // Retrieve the driver from the key-value map
                String key = blockId.toString() + ":" + driverId.toString();
                Map<String, String> driver = drivers.get(key);
                if (driver == null) driver = new HashMap<>();
                // Update the driver information
                driver.put("latitude", dLatitude.toString());
                driver.put("longitude", dLongitude.toString());
                driver.put("gender", dGender);
                driver.put("rating", String.valueOf((dRating + uRating) / 2.0));
                driver.put("salary", dSalary.toString());
                driver.put("status", "AVAILABLE");
                drivers.put(key, driver);
            }
        }
    }

    private Double clientDriverDistance(Double cLat, Double cLng, Double dLat, Double dLng) {
        return Math.sqrt(Math.pow(cLat - dLat, 2) + Math.pow(cLng - dLng, 2));
    }
}
