/*
 * Copyright 2020-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.CriticalPathRetryingKafkaStreamsDelegate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;

public class KafkaStreamsRetryTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true);

	private static final EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private final static CountDownLatch LATCH1 = new CountDownLatch(3);
	private final static CountDownLatch LATCH2 = new CountDownLatch(3);

	@Test
	public void testKafkaStreamsCriticalPathRetryWithTerminalOperation() throws Exception {
		SpringApplication app = new SpringApplication(CriticalPathRetryTestsApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process",
				"--spring.cloud.stream.bindings.process-in-0.destination=words",
				"--spring.cloud.stream.kafka.streams.binder.critical-path-retry.max-attempts=3",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testKafkaStreamsCriticalPathRetryWithTerminalOperation",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			sendAndValidate("words", LATCH1);

		}
	}

	@Test
	public void testKafkaStreamsCriticalPathRetryWithNonTerminalOperation() throws Exception {
		SpringApplication app = new SpringApplication(CriticalPathRetryTestsApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=functionProcess",
				"--spring.cloud.stream.bindings.functionProcess-in-0.destination=words1",
				"--spring.cloud.stream.kafka.streams.binder.critical-path-retry.max-attempts=3",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testKafkaStreamsCriticalPathRetryWithNonTerminalOperation",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			sendAndValidate("words1", LATCH2);

		}
	}

	private void sendAndValidate(String in, CountDownLatch latch) throws InterruptedException {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(in);
			template.sendDefault("foobar");
			Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Foo");
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	public static class CriticalPathRetryTestsApplication {

		@Bean
		public java.util.function.Consumer<KStream<Object, String>> process(CriticalPathRetryingKafkaStreamsDelegate<?, ?> criticalPathRetryingKafkaStreamsDelegate) {

			return input -> input
					.process(() -> new Processor<Object, String>() {
						@Override
						public void init(ProcessorContext processorContext) {
						}

						@Override
						public void process(Object o, String s) {
							criticalPathRetryingKafkaStreamsDelegate.retryCriticalPath(() -> {
								LATCH1.countDown();
								throw new RuntimeException();
							});
						}

						@Override
						public void close() {
						}
					});
		}

		@Bean
		public java.util.function.Function<KStream<Object, String>, KStream<Object, String>> functionProcess(CriticalPathRetryingKafkaStreamsDelegate<Object, String> criticalPathRetryingKafkaStreamsDelegate) {

			return input ->
					input.transform(() -> new Transformer<Object, String, KeyValue<Object, String>>() {
						@Override
						public void init(ProcessorContext processorContext) {
						}

						@Override
						public KeyValue<Object, String> transform(Object o, String s) {
							return criticalPathRetryingKafkaStreamsDelegate.retryCriticalPathAndTransform(() -> {
								LATCH2.countDown();
								throw new RuntimeException();
							});
						}

						@Override
						public void close() {
						}
					})
					.map(KeyValue::new);
		}
	}
}
