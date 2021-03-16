package com.schema.registry.trns.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import example.avro.sample3.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ProducerDemoTopicRecordNameStrategy {
	public static void main(String[] args) {
		Properties properties = new Properties();
		// normal producer
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("acks", "all");
		properties.setProperty("retries", "10");
		// avro part
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
		properties.setProperty("auto.register.schemas", "false");
		properties.setProperty("use.latest.version", "true");
		properties.setProperty("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

		Producer<String, User> producer = new KafkaProducer<String, User>(properties);

		String topic = "user-topic-record-name-strategy-test-1";

		// copied from avro examples
		User user = User.newBuilder()
				.setName("John Doe")
				.build();

		ProducerRecord<String, User> producerRecord = new ProducerRecord<>(
				topic, user
		);

		System.out.println(user);
		producer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception == null) {
					System.out.println(metadata);
				} else {
					exception.printStackTrace();
				}
			}
		});

		producer.flush();
		producer.close();
	}
}
