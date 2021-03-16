package com.schema.registry.trns.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import example.avro.sample3.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class ConsumerDemoTopicRecordNameStrategy {
	public static void main(String[] args) {
		Properties properties = new Properties();
		// normal consumer
		properties.setProperty("bootstrap.servers","127.0.0.1:9092");
		properties.put("group.id", "user-consumer-group-topic-record-name-strategy-1");
		properties.put("auto.commit.enable", "false");
		properties.put("auto.offset.reset", "earliest");

		// avro part (deserializer)
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
		properties.setProperty("specific.avro.reader", "true");
		properties.setProperty("auto.register.schemas", "false");
		properties.setProperty("use.latest.version", "true");
		properties.setProperty("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

		KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer<>(properties);
		String topic = "user-topic-record-name-strategy-test-1";
		kafkaConsumer.subscribe(Collections.singleton(topic));

		System.out.println("Waiting for data...");

		while (true) {
			System.out.println("Polling");
			ConsumerRecords<String, User> records = kafkaConsumer.poll(Duration.ofSeconds(10));

			for (ConsumerRecord<String, User> record : records){
				User user = record.value();
				System.out.println(user);
			}

			kafkaConsumer.commitSync();
		}
	}
}
