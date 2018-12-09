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
        /**
         * All the messages are partitioned by blockId, which means the messages
         * sharing the same blockId will arrive at the same task, similar to the
         * approach that MapReduce sends all the key value pairs with the same key
         * into the same reducer.
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
        /**
         * Update the location of driver.
         */
        // Extract blockId, driverId, latitude, longitude
        Integer blockId = (Integer) driverLocation.get("blockId");
        Integer driverId = (Integer) driverLocation.get("driverId");
        Double latitude = (Double) driverLocation.get("latitude");
        Double longitude = (Double) driverLocation.get("longitude");
        // Construct the key by concatenating the blockId and driverId
        String key = blockId.toString() + ":" + driverId.toString();
        // get or default
        Map<String, String> driver = drivers.get(key);
        if (driver == null) driver = new HashMap<>();
        // Update location
        driver.put("latitude", latitude.toString());
        driver.put("longitude", longitude.toString());
        drivers.put(key, driver);
    }

    private void processEvent(Map<String, Object> event, MessageCollector collector) {
        /**
         * If it is a RIDE_REQUEST, then find the most matched driver;
         * If it is LEAVING_BLOCK, then delete the driver from the previous block;
         * If it is ENTERING_BLOCK, then update/add the information of the driver into KeyValueStore;
         * If it is a RIDE_COMPLETE, then update the rating and other information.
         */
        Integer blockId = (Integer) event.get("blockId");
        String type = (String) event.get("type");
        if (type.equals("RIDE_REQUEST")) {
            findDriver(event, collector, blockId);
        } else {
            Integer driverId = (Integer) event.get("driverId");
            Double dLatitude = (Double) event.get("latitude");
            Double dLongitude = (Double) event.get("longitude");
            if (type.equals("LEAVING_BLOCK")) {
                drivers.delete(blockId.toString() + ":" + driverId.toString());
            } else if (type.equals("ENTERING_BLOCK")) {
                enteringBlock(event, blockId, driverId, dLatitude, dLongitude);
            } else if (type.equals("RIDE_COMPLETE")) {
                rideComplete(event, blockId, driverId, dLatitude, dLongitude);
            }
        }
    }

    private Double clientDriverDistance(Double cLat, Double cLng, Double dLat, Double dLng) {
        /**
         * Compute the Euclidean distance between two coordinates.
         */
        return Math.sqrt(Math.pow(cLat - dLat, 2) + Math.pow(cLng - dLng, 2));
    }

    private void findDriver(Map<String, Object> event, MessageCollector collector, Integer blockId) {
        /**
         * Find the driver that has the highest match score.
         */
        Integer clientId = (Integer) event.get("clientId");
        Double latitude = (Double) event.get("latitude");
        Double longitude = (Double) event.get("longitude");
        String genderPreference = (String) event.get("gender_preference");
        // Search for all drivers with the same blockId
        KeyValueIterator<String, Map<String, String>> candidates = drivers.range(blockId.toString() + ":",
                                                                                blockId.toString() + ";");
        Integer matchedId = -1;
        Double maxMatchScore = 0.0;
        // For every candidates, compute the score and see if it has the highest match score
        while (candidates.hasNext()) {
            try {
                Entry<String, Map<String, String>> cand = candidates.next();
                Map<String, String> driver = cand.getValue();
                // Extract driver information from the hashmap
                String dStatus = driver.get("status");
                if (dStatus.equals("UNAVAILABLE")) continue;
                // The driver is available
                Integer driverId = Integer.valueOf(cand.getKey().split(":")[1]);
                Double dLatitude = Double.valueOf(driver.get("latitude"));
                Double dLongitude = Double.valueOf(driver.get("longitude"));
                String dGender = driver.get("gender");
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
        // Remove the driver from the KeyValueStore
        drivers.delete(blockId.toString() + ":" + matchedId.toString());
        // Send the driver-client pair information
        HashMap<String, Integer> pair = new HashMap<>();
        pair.put("clientId", clientId);
        pair.put("driverId", matchedId);
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, pair));
    }

    private void enteringBlock(Map<String, Object> event, Integer blockId, Integer driverId, Double dLatitude, Double dLongitude) {
        /**
         * The car is entering the block. Simply update the information.
         */
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
    }

    private void rideComplete(Map<String, Object> event, Integer blockId, Integer driverId, Double dLatitude, Double dLongitude) {
        /**
         * The ride is complete. Need to update the rating and other information.
         */
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
