package com.kafka.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "kafka-consumer")
@Component
public class KafkaConsumerConfiguration {

    private String bootstrap_servers;
    private String group_id;
    private String auto_offset_reset;

    public String getBootstrap_servers() {
        return bootstrap_servers;
    }

    public void setBootstrap_servers(String bootstrap_servers) {
        this.bootstrap_servers = bootstrap_servers;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getAuto_offset_reset() {
        return auto_offset_reset;
    }

    public void setAuto_offset_reset(String auto_offset_reset) {
        this.auto_offset_reset = auto_offset_reset;
    }
}
