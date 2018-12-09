
package com.cloudcomputing.samza.ny_cabs;

import org.apache.samza.system.SystemStream;

public class AdMatchConfig {
    public static final SystemStream EVENT_STREAM = new SystemStream("kafka", "events");
    public static final SystemStream AD_STREAM = new SystemStream("kafka", "ad-stream");
}
