package com.bala.kafka.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bala.kafka.advice.PracticalAdvice;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final String topicName;
	private final int messagePerRequest;
	private CountDownLatch latch;

	public KafkaController(KafkaTemplate<String, Object> kafkaTemplate, @Value("${kafka.topic-name}") String topicName,
			@Value("${kafka.messages-per-request}") int messagePerRequest) {
		this.kafkaTemplate = kafkaTemplate;
		this.topicName = topicName;
		this.messagePerRequest = messagePerRequest;
	}

	@GetMapping("/sample")
	private ResponseEntity<String> getSampleOutput() throws Exception {
		latch = new CountDownLatch(messagePerRequest);
		IntStream.range(0, messagePerRequest).forEach(t -> this.kafkaTemplate.send(topicName, String.valueOf(t),
				new PracticalAdvice("A Practical Advice", t)));
		latch.await(60, TimeUnit.SECONDS);
		logger.info("All Messages Received");
		return ResponseEntity.ok().build();
	}

	@KafkaListener(topics = "SPRING-BOOT-SAMPLE-TEST_TOPIC", clientIdPrefix = "json", containerFactory = "kafkaListenerContainerFactory")
	public void listenAsObject(ConsumerRecord<String, PracticalAdvice> cr, @Payload PracticalAdvice payload) {
		logger.info("Logger 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	@KafkaListener(topics = "SPRING-BOOT-SAMPLE-TEST_TOPIC", clientIdPrefix = "string", containerFactory = "kafkaListenerStringContainerFactory")
	public void listenasString(ConsumerRecord<String, String> cr, @Payload String payload) {
		logger.info("Logger 2 [String] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	@KafkaListener(topics = "SPRING-BOOT-SAMPLE-TEST_TOPIC", clientIdPrefix = "bytearray", containerFactory = "kafkaListenerByteArrayContainerFactory", groupId = "spring-loggers")
	public void listenAsByteArray(ConsumerRecord<String, byte[]> cr, @Payload byte[] payload) {
		logger.info("Logger 3 [ByteArray] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
				typeIdHeader(cr.headers()), payload, cr.toString());
		latch.countDown();
	}

	private static String typeIdHeader(Headers headers) {
		return StreamSupport.stream(headers.spliterator(), false).filter(header -> header.key().equals("__TypeId__"))
				.findFirst().map(header -> new String(header.value())).orElse("N/A");
	}
}
