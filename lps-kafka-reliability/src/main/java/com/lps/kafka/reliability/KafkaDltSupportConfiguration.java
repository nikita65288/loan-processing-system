package com.lps.kafka.reliability;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Imports one shared DLT producer per service; no additional always-running consumer. */
@Configuration(proxyBeanMethods = false)
public class KafkaDltSupportConfiguration {
    @Bean(destroyMethod = "close")
    public KafkaDltSupport kafkaDltSupport(
            @Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${spring.application.name}") String service,
            MeterRegistry meters) {
        return new KafkaDltSupport(brokers, service, meters);
    }
}
