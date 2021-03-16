package com.schema.registry.multievents.tns.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.sample.test.UserCreatedEvent;
import com.sample.test.UserDeletedEvent;
import example.avro.sample1.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;

public class ConsumerDemoTopicNameStrategyME {

	public static void main(String[] args) {
		Properties properties = new Properties();
		// normal consumer
		properties.setProperty("bootstrap.servers","127.0.0.1:9092");
		properties.put("group.id", "mult-event-topic-tns-group");
		properties.put("auto.commit.enable", "false");
		properties.put("auto.offset.reset", "earliest");

		// avro part (deserializer)
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

		properties.setProperty("auto.register.schemas", "false");
		properties.setProperty("use.latest.version", "true");
		properties.setProperty("specific.avro.reader", "true");
		properties.setProperty("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");

		KafkaConsumer<String, SpecificRecord> kafkaConsumer = new KafkaConsumer<>(properties);
		String topic = "mult-event-topic-tns";
		kafkaConsumer.subscribe(Collections.singleton(topic));

		System.out.println("Waiting for data...");

		while (true) {
			System.out.println("Polling");
			ConsumerRecords<String, SpecificRecord> records = kafkaConsumer.poll(Duration.ofSeconds(1));

			for (ConsumerRecord<String, SpecificRecord> record : records){
				final SpecificRecord value = record.value();
				if (value instanceof UserCreatedEvent) {
					System.out.println("UserCreatedEvent:" + value);
				} else if (value instanceof UserDeletedEvent) {
					System.out.println("UserDeletedEvent:" + value);
				} else {
					System.out.println("Unknow type" + value);
				}

			}

			kafkaConsumer.commitSync();
		}
	}
}
