/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.binder.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Metrics for Kafka binder.
 *
 * @author Henryk Konsek
 */
public class KafkaBinderMetrics implements PublicMetrics {

	static final String METRIC_PREFIX = "spring.stream.binder.kafka.topic.lag.";

	private final KafkaMessageChannelBinder binder;

	private final ConsumerFactory<?, ?> consumerFactory;

	public KafkaBinderMetrics(KafkaMessageChannelBinder binder, ConsumerFactory<?, ?> consumerFactory) {
		this.binder = binder;
		this.consumerFactory = consumerFactory;
	}

	@Override public Collection<Metric<?>> metrics() {
		List<Metric<?>> metrics = new LinkedList<>();
		try (Consumer<?, ?> metadataConsumer = consumerFactory.createConsumer()) {
			for (Map.Entry<String, KafkaMessageChannelBinder.TopicInformation> topicInfo : this.binder.getTopicsInUse().entrySet()) {
				if(!topicInfo.getValue().isConsumerTopic()) {
					continue;
				}
				String topic = topicInfo.getKey();
				List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);
				List<TopicPartition> topicPartitions = new LinkedList<>();
				for(PartitionInfo partitionInfo : partitionInfos) {
					topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}
				Map<TopicPartition, Long> endOffsets = metadataConsumer.endOffsets(topicPartitions);
				long lag = 0;
				for(Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
					OffsetAndMetadata current = metadataConsumer.committed(endOffset.getKey());
					if(current != null) {
						lag += endOffset.getValue() - current.offset();
					}
				}
				metrics.add(new Metric<>(METRIC_PREFIX + topic, lag));
			}
			return metrics;
		}
		catch (Exception e) {
			return emptyList();
		}
	}

}