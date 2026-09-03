package com.banking.transactionservice.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaErrorConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory() {

        //create kafka producers and tells it is running at 9092 port

        Map<String, Object> props = new HashMap<>();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        /*kafka sends the bytes and java object need sericalizaiton.
        Normal application object
        ↓
        JacksonJsonSerializer

        Raw byte[]
                ↓
        ByteArraySerializer

        byte serializer also comes in handy since failed topic is published to DLT

         */
        DelegatingByTypeSerializer serializer =
                new DelegatingByTypeSerializer(
                        Map.of(
                                byte[].class,
                                new ByteArraySerializer(),

                                Object.class,
                                new JacksonJsonSerializer<>()
                        ),
                        true
                );

        return new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                serializer
        );
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }


    /* This helps in publishing failed topic to DLT

        fraud.check.clean
            ↓
    Transaction Service consumer
            ↓
        Exception
            ↓
          Retry
            ↓
          Retry
            ↓
    Still failing
            ↓
    fraud.check.clean-dlt

    waut for 1000ms i.e. 1sec and do 2 retry attempts before pushing to DLT
    hence 1 original + 2 retry = 3 total attempts
     */

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}