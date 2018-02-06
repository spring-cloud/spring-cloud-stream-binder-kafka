/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream.config;

import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.BoundedKStreamPropertyCache;
import org.springframework.cloud.stream.binder.kstream.KStreamBoundElementFactory;
import org.springframework.cloud.stream.binder.kstream.KStreamBoundMessageConversionDelegate;
import org.springframework.cloud.stream.binder.kstream.KStreamListenerParameterAdapter;
import org.springframework.cloud.stream.binder.kstream.KStreamListenerSetupMethodOrchestrator;
import org.springframework.cloud.stream.binder.kstream.KStreamStreamListenerResultAdapter;
import org.springframework.cloud.stream.binder.kstream.KeyValueSerdeResolver;
import org.springframework.cloud.stream.binder.kstream.SendToDlqAndContinue;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
@EnableConfigurationProperties(KStreamExtendedBindingProperties.class)
public class KStreamBinderSupportAutoConfiguration {

	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kstream.binder")
	public KStreamBinderConfigurationProperties binderConfigurationProperties() {
		return new KStreamBinderConfigurationProperties();
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean defaultKafkaStreamBuilder(
			@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME) ObjectProvider<StreamsConfig> streamsConfigProvider) {
		StreamsConfig streamsConfig = streamsConfigProvider.getIfAvailable();
		if (streamsConfig != null) {
			StreamsBuilderFactoryBean kStreamBuilderFactoryBean = new StreamsBuilderFactoryBean(streamsConfig);
			kStreamBuilderFactoryBean.setPhase(Integer.MAX_VALUE - 500);
			return kStreamBuilderFactoryBean;
		} else {
			throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
					KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME, "streamsConfig",
					"There is no '" + KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME
							+ "' StreamsConfig bean in the application context.\n");
		}
	}

	@Bean(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig streamsConfig(KStreamBinderConfigurationProperties binderConfigurationProperties, AbstractApplicationContext applicationContext) {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, binderConfigurationProperties.getKafkaConnectionString());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "default");

		if(binderConfigurationProperties.getOnDeserializationError().logAndContinue) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		}
		else if(binderConfigurationProperties.getOnDeserializationError().logAndFail) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		}
		else if (binderConfigurationProperties.getOnDeserializationError().sendToDlq) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					SendToDlqAndContinue.class);
		}

		if (!ObjectUtils.isEmpty(binderConfigurationProperties.getConfiguration())) {
			props.putAll(binderConfigurationProperties.getConfiguration());
		}
		props.put("spring.application.context", applicationContext);


		StreamsConfig streamsConfig = new StreamsConfig(props) {

			DeserializationExceptionHandler deserializationExceptionHandler;

			@Override
			@SuppressWarnings("unchecked")
			public <T> T getConfiguredInstance(String key, Class<T> t) {
				if (key.equals(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)){
					if (deserializationExceptionHandler != null){
						return (T)deserializationExceptionHandler;
					}
					else {
						T t1 = super.getConfiguredInstance(key, t);
						deserializationExceptionHandler = (DeserializationExceptionHandler)t1;
						return t1;
					}
				}
				return super.getConfiguredInstance(key, t);
			}
		};

		return streamsConfig;
	}

	@Bean
	public KStreamStreamListenerResultAdapter kafkaStreamStreamListenerResultAdapter() {
		return new KStreamStreamListenerResultAdapter();
	}

	@Bean
	public KStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter(
			KStreamBoundMessageConversionDelegate kstreamBoundMessageConversionDelegate, BoundedKStreamPropertyCache boundedKStreamPropertyCache) {
		return new KStreamListenerParameterAdapter(kstreamBoundMessageConversionDelegate, boundedKStreamPropertyCache);
	}

	@Bean
	public KStreamListenerSetupMethodOrchestrator kStreamListenerSetupMethodOrchestrator(
			KStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter,
			Collection<StreamListenerResultAdapter> streamListenerResultAdapters){
		return new KStreamListenerSetupMethodOrchestrator(kafkaStreamListenerParameterAdapter, streamListenerResultAdapters);
	}

	@Bean
	public KStreamBoundMessageConversionDelegate messageConversionDelegate(CompositeMessageConverterFactory compositeMessageConverterFactory,
																		SendToDlqAndContinue sendToDlqAndContinue,
																		KStreamBinderConfigurationProperties kStreamBinderConfigurationProperties,
																		BoundedKStreamPropertyCache boundedKStreamPropertyCache) {
		return new KStreamBoundMessageConversionDelegate(compositeMessageConverterFactory, sendToDlqAndContinue,
				kStreamBinderConfigurationProperties, boundedKStreamPropertyCache);
	}

	@Bean
	public KStreamBoundElementFactory kafkaStreamBindableTargetFactory(StreamsBuilder kStreamBuilder,
																	BindingServiceProperties bindingServiceProperties,
																	BoundedKStreamPropertyCache boundedKStreamPropertyCache,
																	KeyValueSerdeResolver keyValueSerdeResolver) {
		return new KStreamBoundElementFactory(kStreamBuilder, bindingServiceProperties,
				boundedKStreamPropertyCache, keyValueSerdeResolver);
	}

	@Bean
	public SendToDlqAndContinue kStreamDlqSender() {
		return new SendToDlqAndContinue();
	}

	@Bean
	public BoundedKStreamPropertyCache kStreamBoundTargetInformation() {
		return new BoundedKStreamPropertyCache();
	}

	@Bean
	public KeyValueSerdeResolver keyValueSerdeResolver(StreamsConfig streamsConfig, BindingServiceProperties bindingServiceProperties,
													KStreamBinderConfigurationProperties kStreamBinderConfigurationProperties,
													KStreamExtendedBindingProperties kStreamExtendedBindingProperties) {
		return new KeyValueSerdeResolver(streamsConfig, bindingServiceProperties,
				kStreamBinderConfigurationProperties, kStreamExtendedBindingProperties);
	}
}
