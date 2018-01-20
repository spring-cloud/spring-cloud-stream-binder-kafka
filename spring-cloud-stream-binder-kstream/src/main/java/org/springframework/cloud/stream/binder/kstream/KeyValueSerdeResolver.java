/*
 * Copyright 2018 the original author or authors.
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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 */
public class KeyValueSerdeResolver {

	private final BindingServiceProperties bindingServiceProperties;

	private final KStreamExtendedBindingProperties kStreamExtendedBindingProperties;

	private final StreamsConfig streamsConfig;

	private final KStreamBinderConfigurationProperties binderConfigurationProperties;

	public KeyValueSerdeResolver(StreamsConfig streamsConfig,
								BindingServiceProperties bindingServiceProperties,
								KStreamBinderConfigurationProperties binderConfigurationProperties,
								KStreamExtendedBindingProperties kStreamExtendedBindingProperties) {
		this.streamsConfig = streamsConfig;
		this.bindingServiceProperties = bindingServiceProperties;
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
	}

	public Serde<?> getInboundKeySerde(String binding) {
		KStreamConsumerProperties extendedConsumerProperties =
				kStreamExtendedBindingProperties.getExtendedConsumerProperties(binding);
		String keySerdeString = extendedConsumerProperties.getKeySerde();

		return getKeySerde(keySerdeString);
	}

	public Serde<?> getOuboundKeySerde(ExtendedProducerProperties<KStreamProducerProperties> properties) {
		return getKeySerde(properties.getExtension().getKeySerde());
	}

	private Serde<?> getKeySerde(String keySerdeString) {
		Serde<?> keySerde;
		try {
			if (StringUtils.hasText(keySerdeString)) {
				keySerde = Utils.newInstance(keySerdeString, Serde.class);
				if (keySerde instanceof Configurable) {
					((Configurable) keySerde).configure(streamsConfig.originals());
				}
			} else {
				keySerde = this.binderConfigurationProperties.getConfiguration().containsKey("key.serde") ?
						Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("key.serde"), Serde.class) : Serdes.ByteArray();
			}

		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return keySerde;
	}

	public Serde<?> getInboundValueSerde(String binding) {
		Serde<?> valueSerde;

		KStreamConsumerProperties extendedConsumerProperties =
				kStreamExtendedBindingProperties.getExtendedConsumerProperties(binding);
		String valueSerdeString = extendedConsumerProperties.getValueSerde();
		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(binding);
		try {
			if (bindingProperties.getConsumer() != null &&
					bindingProperties.getConsumer().isUseNativeDecoding()) {
				valueSerde = getValueSerde(valueSerdeString);
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return valueSerde;
	}

	public Serde<?> getOutboundValueSerde(ExtendedProducerProperties<KStreamProducerProperties> properties) {
		Serde<?> valueSerde;
		try {
			if (properties.isUseNativeEncoding()) {
				valueSerde = getValueSerde(properties.getExtension().getValueSerde());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Serde class not found: ", e);
		}
		return valueSerde;
	}

	private Serde<?> getValueSerde(String valueSerdeString) throws ClassNotFoundException {
		Serde<?> valueSerde;
		if (StringUtils.hasText(valueSerdeString)) {
			valueSerde = Utils.newInstance(valueSerdeString, Serde.class);
			if (valueSerde instanceof Configurable) {
				((Configurable) valueSerde).configure(streamsConfig.originals());
			}
		}
		else {
			valueSerde = this.binderConfigurationProperties.getConfiguration().containsKey("value.serde") ?
					Utils.newInstance(this.binderConfigurationProperties.getConfiguration().get("value.serde"), Serde.class) : Serdes.ByteArray();
		}
		return valueSerde;
	}
}
