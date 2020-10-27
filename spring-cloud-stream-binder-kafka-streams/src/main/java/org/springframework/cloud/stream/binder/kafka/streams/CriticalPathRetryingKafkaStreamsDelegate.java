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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.concurrent.Callable;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.lang.Nullable;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * This class is part of the public API of Kafka Streams binder.
 * <p>
 * It provides a way for Kafka Streams applications to retry certain parts of the logic that are in the critical path.
 * Applications that need to make external calls during runtime such as a relational database lookup, REST endpoint
 * invocation etc. are much more susceptible to fail for wide array of reasons. When those calls throw an exception,
 * the Kafka Streams processor will fail and shuts itself down, thus those calls stand in the critical path of the
 * application. It is often handy, if the application could retry the failing operation in the event of a network failure,
 * bad connection or a remote service is temporarily unavailable etc. This API essentially allows the application to
 * wrap the critical path code in a {@link Callable} implementation and retry it based on binder level retry properties.
 * This will use Spring's {@link RetryTemplate} under the hood to invoke the Callable implementation.
 * <p>
 * It supports both terminal ({@link org.apache.kafka.streams.processor.Processor}) and non-terminal
 * ({@link org.apache.kafka.streams.kstream.Transformer} operations to be wrapped inside the {@link Callable} implementation.
 * <p>
 * See the relevant API methods for more information.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author Soby Chacko
 * @since 3.0.9
 */
public class CriticalPathRetryingKafkaStreamsDelegate<K, V> {

	private final KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties;

	public CriticalPathRetryingKafkaStreamsDelegate(@Nullable KafkaStreamsBinderConfigurationProperties kafkaStreamsBinderConfigurationProperties,
													KafkaProperties kafkaProperties) {
		this.kafkaStreamsBinderConfigurationProperties = kafkaStreamsBinderConfigurationProperties == null ?
				new KafkaStreamsBinderConfigurationProperties(kafkaProperties) : kafkaStreamsBinderConfigurationProperties;
	}

	/**
	 * This method allows an application to wrap any failure-prone critical functions within a {@link Callable}
	 * that can be retried using configured settings. This API method is a terminal operation and recommended to be
	 * used within the context of a {@link org.apache.kafka.streams.processor.Processor}. Once the process method is
	 * called with the {@link Callable}, it does not return anything back to the caller, rather the processing is
	 * terminated for the current {@link KStream}.
	 * <p>
	 * Here is an example of how this API can be used.
	 * <p>
	 * &#064;Bean
	 * public java.util.function.Consumer&lt;KStream&gt; process(CriticalPathRetryingKafkaStreamsDelegate criticalPathRetryingKafkaStreamsDelegate) {
	 * return input -&gt; input
	 * .process(() -&gt; new Processor() {
	 * &#064;Override
	 * public void init(ProcessorContext processorContext) {
	 * }
	 * &#064;Override
	 * public void process(Object o, String s) {
	 * criticalPathRetryingKafkaStreamsDelegate.retryCriticalPath(() -&gt; {
	 * // Application's business logic that is on the critical path.
	 * });
	 * }
	 * &#064;Override
	 * public void close() {
	 * }
	 * });
	 * }
	 * <p>
	 * The {@link Callable} implementation that is provided as a lambda expression to the Kafka Streams process method
	 * is retied based on teh configured retry properties that can be set at the binder level.
	 * <p>
	 * See {@link KafkaStreamsBinderConfigurationProperties.CriticalPathRetry} for details on the available retry properties,
	 * those are used for creating the {@link RetryTemplate}.
	 *
	 * @param callable {@link Callable} implementation that wraps the critical path business logic.
	 */
	public void retryCriticalPath(Callable<V> callable) {

		getRetryTemplate().execute(context -> {
			try {
				callable.call();
			}
			catch (Exception exception) {
				throw new IllegalStateException(exception);
			}
			return null;
		});
	}

	/**
	 * This method allows an application to wrap any failure-prone critical functions within a {@link Callable}
	 * that can be retried using configured settings. This API method is a non-terminal operation and recommended to be
	 * used within the context of a {@link org.apache.kafka.streams.kstream.Transformer}. Once the transform method is
	 * called with the {@link Callable}, it returns a {@link KeyValue} pair using which further processing can
	 * continue downstream.
	 * <p>
	 * Here is an example of how this API can be used.
	 * <p>
	 * &#064;Bean
	 * public java.util.function.Function&lt;KStream&lt;Object, String&gt;, KStream&lt;Object, String&gt;&gt; process(CriticalPathRetryingKafkaStreamsDelegate criticalPathRetryingKafkaStreamsDelegate) {
	 * return input -&gt;
	 * input.transform(() -&gt; new Transformer&lt;Object, String, KeyValue&lt;Object, String&gt;&gt;() {
	 * &#064;Override
	 * public void init(ProcessorContext processorContext) {
	 * }
	 * <p>
	 * &#064;Override
	 * public KeyValue&lt;Object, String&gt; transform(Object o, String s) {
	 * return criticalPathRetryingKafkaStreamsDelegate.retryCriticalPathAndTransform(() -&gt; {
	 * // Application's business logic that is on the critical path.
	 * return new KeyValue(...)
	 * });
	 * }
	 * <p>
	 * &#064;Override
	 * public void close() {
	 * }                    * 					})
	 * .map(new KeyValue(...));
	 * }
	 * }
	 * <p>
	 * The {@link Callable} implementation that is provided as a lambda expression to the Kafka Streams transform method
	 * is retried based on the configured retry properties that can be set at the binder level.
	 * <p>
	 * See {@link KafkaStreamsBinderConfigurationProperties.CriticalPathRetry} for details on the available retry properties,
	 * those are used for creating the {@link RetryTemplate}.
	 *
	 * @param callable {@link Callable} implementation that wraps the critical path business logic.
	 * @return {@link KeyValue} from the transformation result (It may be the same one that is passed into the method).
	 */
	public KeyValue<K, V> retryCriticalPathAndTransform(Callable<KeyValue<K, V>> callable) {

		try {
			return getRetryTemplate().execute(context -> {
				try {
					return callable.call();
				}
				catch (Exception exception) {
					throw new IllegalStateException(exception);
				}
			});
		}
		catch (Exception e) {
			return null;
		}
	}

	private RetryTemplate getRetryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();

		final KafkaStreamsBinderConfigurationProperties.CriticalPathRetry criticalPathRetry =
				this.kafkaStreamsBinderConfigurationProperties.getCriticalPathRetry();
		RetryPolicy retryPolicy = new SimpleRetryPolicy(
				criticalPathRetry.getMaxAttempts());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(criticalPathRetry.getBackoffPeriod());

		retryTemplate.setBackOffPolicy(backOffPolicy);
		retryTemplate.setRetryPolicy(retryPolicy);

		return retryTemplate;
	}
}
