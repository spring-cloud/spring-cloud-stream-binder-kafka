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

package org.springframework.cloud.stream.binder.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KStreamExtendedBindingProperties kStreamExtendedBindingProperties;

	private final StreamsConfig streamsConfig;

	private final KStreamBinderConfigurationProperties binderConfigurationProperties;

	private final KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate;

	private final BoundedKStreamPropertyCache boundedKStreamPropertyCache;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	public KStreamBinder(KStreamBinderConfigurationProperties binderConfigurationProperties,
						KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamExtendedBindingProperties kStreamExtendedBindingProperties, StreamsConfig streamsConfig,
						KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate, BoundedKStreamPropertyCache boundedKStreamPropertyCache, KeyValueSerdeResolver keyValueSerdeResolver) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
		this.streamsConfig = streamsConfig;
		this.kStreamBoundMessageConversionDelegate = kStreamBoundMessageConversionDelegate;
		this.boundedKStreamPropertyCache = boundedKStreamPropertyCache;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
	}

	@Override
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
															KStream<Object, Object> inputTarget,
															ExtendedConsumerProperties<KStreamConsumerProperties> properties) {

		this.boundedKStreamPropertyCache.addBindingTargetToConsumerProperties(inputTarget, properties.getExtension());
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				properties.getExtension());
		if (binderConfigurationProperties.getOnDeserializationError().isSendToDlq()) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);

		if (extendedConsumerProperties.getExtension().isEnableDlq()) {
			String dlqName = StringUtils.isEmpty(extendedConsumerProperties.getExtension().getDlqName()) ?
					"error." + name + "." + group : extendedConsumerProperties.getExtension().getDlqName();
			KStreamDlqDispatch kStreamDlqDispatch = new KStreamDlqDispatch(dlqName, binderConfigurationProperties,
					extendedConsumerProperties.getExtension());
			SendToDlqAndContinue sendToDlqAndContinue = this.getApplicationContext().getBean(SendToDlqAndContinue.class);
			sendToDlqAndContinue.addKStreamDlqDispatch(name, kStreamDlqDispatch);

			DeserializationExceptionHandler deserializationExceptionHandler = streamsConfig.defaultDeserializationExceptionHandler();
			if(deserializationExceptionHandler instanceof SendToDlqAndContinue) {
				((SendToDlqAndContinue)deserializationExceptionHandler).addKStreamDlqDispatch(name, kStreamDlqDispatch);
			}
		}
		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name, KStream<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KStreamProducerProperties> properties) {
		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		this.kafkaTopicProvisioner.provisionProducerDestination(name, extendedProducerProperties);

		Serde<?> keySerde = this.keyValueSerdeResolver.getOuboundKeySerde(properties);
		Serde<?> valueSerde = this.keyValueSerdeResolver.getOutboundValueSerde(properties);

		to(properties.isUseNativeEncoding(), name, outboundBindTarget, (Serde<Object>) keySerde, (Serde<Object>) valueSerde);

		return new DefaultBinding<>(name, null, outboundBindTarget, null);
	}

	@SuppressWarnings("unchecked")
	private void to(boolean isNativeEncoding, String name, KStream<Object, Object> outboundBindTarget,
				Serde<Object> keySerde, Serde<Object> valueSerde) {
		if (!isNativeEncoding) {
			kStreamBoundMessageConversionDelegate.serializeOnOutbound(outboundBindTarget)
					.to(name, Produced.with(keySerde, valueSerde));
		}
		else {
			outboundBindTarget.to(name, Produced.with(keySerde, valueSerde));
		}
	}

	@Override
	public KStreamConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KStreamProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}
}
