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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics.METRIC_PREFIX;

/**
 * @author Henryk Konsek
 */
public class KafkaBinderMetricsTest {

	private static final String TEST_TOPIC = "test";

	private KafkaBinderMetrics metrics;

	@Mock
	private DefaultKafkaConsumerFactory consumerFactory;

	@Mock
	private KafkaConsumer consumer;

	@Mock
	private KafkaMessageChannelBinder binder;

	private Map<String, KafkaMessageChannelBinder.TopicInformation> topicsInUse = new HashMap<>();

	@Mock
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		given(consumerFactory.createConsumer()).willReturn(consumer);
		given(binder.getTopicsInUse()).willReturn(topicsInUse);
		metrics = new KafkaBinderMetrics(binder, kafkaBinderConfigurationProperties, consumerFactory);
		given(consumer.endOffsets(anyCollectionOf(TopicPartition.class))).willReturn(singletonMap(new TopicPartition(TEST_TOPIC, 0), 1000L));
	}

	@Test
	public void shouldIndicateLag() {
		given(consumer.committed(any(TopicPartition.class))).willReturn(new OffsetAndMetadata(500));
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		Collection<Metric<?>> collectedMetrics = metrics.metrics();
		assertThat(collectedMetrics).hasSize(1);
		assertThat(collectedMetrics.iterator().next().getName()).isEqualTo(String.format("%s.%s.%s.lag", METRIC_PREFIX, TEST_TOPIC, "group"));
		assertThat(collectedMetrics.iterator().next().getValue()).isEqualTo(500L);
	}

	@Test
	public void shouldIndicateFullLagForNotCommittedGroups() {
		final List<PartitionInfo> partitions = partitions(new Node(0, null, 0));
		topicsInUse.put(TEST_TOPIC, new KafkaMessageChannelBinder.TopicInformation("group", partitions));
		given(consumer.partitionsFor(TEST_TOPIC)).willReturn(partitions);
		Collection<Metric<?>> collectedMetrics = metrics.metrics();
		assertThat(collectedMetrics).hasSize(1);
		assertThat(collectedMetrics.iterator().next().getName()).isEqualTo(String.format("%s.%s.%s.lag", METRIC_PREFIX, TEST_TOPIC, "group"));
		assertThat(collectedMetrics.iterator().next().getValue()).isEqualTo(1000L);
	}

	private List<PartitionInfo> partitions(Node leader) {
		List<PartitionInfo> partitions = new ArrayList<>();
		partitions.add(new PartitionInfo(TEST_TOPIC, 0, leader, null, null));
		return partitions;
	}

}
