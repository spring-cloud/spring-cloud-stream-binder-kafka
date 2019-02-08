/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsApplicationSupportProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Arnaud Jardiné
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
@DirtiesContext
public abstract class KafkaStreamsBinderHealthIndicatorTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"out");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	@Autowired
	protected ApplicationContext context;

	@BeforeClass
	public static void setUp() {
		System.setProperty("logging.level.org.apache.kafka", "OFF");
		System.setProperty("management.health.binders.enabled", "true");
		System.setProperty("server.port", "0");
		System.setProperty("spring.jmx.enabled", "false");
		System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers",
				embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.streams.binder.zkNodes",
				embeddedKafka.getZookeeperConnectionString());
		System.setProperty(
				"spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms",
				"1000");
		System.setProperty(
				"spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde",
				"org.apache.kafka.common.serialization.Serdes$StringSerde");
		System.setProperty(
				"spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde",
				"org.apache.kafka.common.serialization.Serdes$StringSerde");

		System.setProperty("spring.cloud.stream.bindings.input.destination", "in");
		System.setProperty("spring.cloud.stream.bindings.output.destination", "out");
		System.setProperty(
				"spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId",
				"ApplicationHealthTest-xyz");
		System.setProperty(
				"spring.cloud.stream.kafka.streams.bindings.output.producer.keySerde",
				"org.apache.kafka.common.serialization.Serdes$IntegerSerde");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "out");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	private static Status getStatusKStream(Map<String, Object> details) {
		Health health = (Health) details.get("kstream");
		return health != null ? health.getStatus() : Status.DOWN;
	}

	@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KStreamApplication.class)
	public static class KafkaStreamSuccessHealthIndicatorUpTest
			extends KafkaStreamsBinderHealthIndicatorTests {

		@Test
		public void test() throws Exception {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("in");
				template.sendDefault("{\"id\":\"123\"}");
				KafkaTestUtils.getSingleRecord(consumer, "out");

				Health health = context
						.getBean("bindersHealthIndicator", HealthIndicator.class)
						.health();
				assertThat(health.getStatus()).isEqualTo(Status.UP);
				assertThat(getStatusKStream(health.getDetails())).isEqualTo(Status.UP);
			}
			finally {
				pf.destroy();
			}
		}

	}

	@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KStreamApplication.class)
	public static class KafkaStreamSuccessHealthIndicatorDownTest
			extends KafkaStreamsBinderHealthIndicatorTests {

		@Test
		public void test() throws Exception {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("in");
			template.sendDefault("{\"id\":\"124\"}");

			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				CountDownLatch latch = new CountDownLatch(1);
				executor.execute(() -> KafkaTestUtils.getSingleRecord(consumer, "out"));
				latch.await(1, TimeUnit.SECONDS);
				Health health = context
						.getBean("bindersHealthIndicator", HealthIndicator.class)
						.health();
				assertThat(health.getStatus()).isEqualTo(Status.DOWN);
				assertThat(getStatusKStream(health.getDetails())).isEqualTo(Status.DOWN);
			}
			finally {
				executor.shutdownNow();
				pf.destroy();
			}
		}

	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	@EnableConfigurationProperties(KafkaStreamsApplicationSupportProperties.class)
	public static class KStreamApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<Object, Product> process(KStream<Object, Product> input) {
			return input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

	}

	static class Product {

		Integer id;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

	}

}
