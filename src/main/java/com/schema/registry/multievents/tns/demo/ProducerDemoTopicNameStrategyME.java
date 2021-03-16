package com.schema.registry.multievents.tns.demo;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.common.event.EventMetadata;
import com.sample.test.UserCreatedEvent;
import com.sample.test.UserDeletedEvent;
import example.avro.sample1.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ProducerDemoTopicNameStrategyME {
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

		Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties);

		String topic = "mult-event-topic-tns";

		// copied from avro examples
		UserCreatedEvent userCreatedEvent = UserCreatedEvent.newBuilder()
				.setUserId("1")
				.setEmail("abc1@ad.com")
				.setEventMetadata(EventMetadata.newBuilder().setType("CREATED").build())
				.build();

		System.out.println(userCreatedEvent);
		ProducerRecord<String, SpecificRecord> userCreatedRecord = new ProducerRecord<>(topic, userCreatedEvent);
		sendMessage(producer, userCreatedRecord);

		UserDeletedEvent userDeletedEventEvent = UserDeletedEvent.newBuilder()
				.setUserId("1")
				.setDeletedBy("Admin")
				.setDeletionType("HARD")
				.setEventMetadata(EventMetadata.newBuilder().setType("DELETED").build())
				.build();
		System.out.println(userDeletedEventEvent);
		ProducerRecord<String, SpecificRecord> userDeletedRecord = new ProducerRecord<>(topic, userDeletedEventEvent);
		sendMessage(producer, userDeletedRecord);

		producer.flush();
		producer.close();
	}

	private static void sendMessage(Producer<String, SpecificRecord> producer, ProducerRecord<String, SpecificRecord> producerRecord) {
		producer.send(producerRecord, (metadata, exception) -> {
			if (exception == null) {
				System.out.println(metadata);
			} else {
				exception.printStackTrace();
			}
		});
	}
}
