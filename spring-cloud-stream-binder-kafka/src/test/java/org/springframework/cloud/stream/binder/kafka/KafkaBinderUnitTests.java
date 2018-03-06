/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.2.2
 *
 */
public class KafkaBinderUnitTests {

	@Test
	public void testPropertyOverrides() throws Exception {
		KafkaBinderConfigurationProperties binderConfigurationProperties = new KafkaBinderConfigurationProperties();
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(binderConfigurationProperties, new KafkaProperties());
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(binderConfigurationProperties,
				provisioningProvider);
		KafkaConsumerProperties consumerProps = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> ecp =
				new ExtendedConsumerProperties<KafkaConsumerProperties>(consumerProps);
		Method method = KafkaMessageChannelBinder.class.getDeclaredMethod("createKafkaConsumerFactory", boolean.class,
				String.class, ExtendedConsumerProperties.class);
		method.setAccessible(true);

		// test default for anon
		Object factory = method.invoke(binder, true, "foo", ecp);
		Map<?, ?> configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("latest");

		// test default for named
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");

		// binder level setting
		binderConfigurationProperties.setConfiguration(
				Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("latest");

		// consumer level setting
		consumerProps.setConfiguration(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
	}

	@Test
	public void testOffsetResetWithGroupManagementEarliest() throws Exception {
		testOffsetResetWithGroupManagement(true, true);
	}

	@Test
	public void testOffsetResetWithGroupManagementLatest() throws Throwable {
		testOffsetResetWithGroupManagement(false, true);
	}

	@Test
	public void testOffsetResetWithManualAssignmentEarliest() throws Exception {
		testOffsetResetWithGroupManagement(true, false);
	}

	@Test
	public void testOffsetResetWithGroupManualAssignmentLatest() throws Throwable {
		testOffsetResetWithGroupManagement(false, false);
	}

	private void testOffsetResetWithGroupManagement(final boolean earliest, boolean groupManage) throws Exception {
		final List<TopicPartition> partitions = new ArrayList<>();
		partitions.add(new TopicPartition("foo", 0));
		partitions.add(new TopicPartition("foo", 1));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties();
		KafkaTopicProvisioner provisioningProvider = mock(KafkaTopicProvisioner.class);
		ConsumerDestination dest = mock(ConsumerDestination.class);
		given(dest.getName()).willReturn("foo");
		given(provisioningProvider.provisionConsumerDestination(anyString(), anyString(), any())).willReturn(dest);
		final AtomicInteger part = new AtomicInteger();
		willAnswer(i -> {
			return partitions.stream()
					.map(p -> new PartitionInfo("foo", part.getAndIncrement(), null, null, null))
					.collect(Collectors.toList());
		}).given(provisioningProvider).getPartitionsForTopic(anyInt(), anyBoolean(), any());
		@SuppressWarnings("unchecked")
		final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(i -> {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return new ConsumerRecords<>(Collections.emptyMap());
		}).given(consumer).poll(anyLong());
		willAnswer(i -> {
			((org.apache.kafka.clients.consumer.ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(partitions);
			latch.countDown();
			latch.countDown();
			return null;
		}).given(consumer).subscribe(eq(Collections.singletonList("foo")),
				any(org.apache.kafka.clients.consumer.ConsumerRebalanceListener.class));
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(consumer).seek(any(), anyLong());
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties, provisioningProvider) {

			@Override
			protected ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous, String consumerGroup,
					ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {

				return new ConsumerFactory<byte[], byte[]>() {

					@Override
					public Consumer<byte[], byte[]> createConsumer() {
						return consumer;
					}

					@Override
					public Consumer<byte[], byte[]> createConsumer(String arg0) {
						return consumer;
					}

					@Override
					public Consumer<byte[], byte[]> createConsumer(String arg0, String arg1) {
						return consumer;
					}

					@Override
					public boolean isAutoCommit() {
						return false;
					}

					@Override
					public Map<String, Object> getConfigurationProperties() {
						return Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
								earliest ? "earliest" : "latest");
					}

				};
			}

		};
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		MessageChannel channel = new DirectChannel();
		KafkaConsumerProperties extension = new KafkaConsumerProperties();
		extension.setResetOffsets(true);
		extension.setAutoRebalanceEnabled(groupManage);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = new ExtendedConsumerProperties<KafkaConsumerProperties>(
				extension);
		consumerProperties.setInstanceCount(1);
		binder.bindConsumer("foo", "bar", channel, consumerProperties);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		if (groupManage) {
			if (earliest) {
				verify(consumer).seekToBeginning(partitions);
			}
			else {
				verify(consumer).seekToEnd(partitions);
			}
		}
		else {
			if (earliest) {
				verify(consumer).seek(partitions.get(0), 0L);
				verify(consumer).seek(partitions.get(1), 0L);
			}
			else {
				verify(consumer).seek(partitions.get(0), Long.MAX_VALUE);
				verify(consumer).seek(partitions.get(1), Long.MAX_VALUE);
			}
		}
	}


}
