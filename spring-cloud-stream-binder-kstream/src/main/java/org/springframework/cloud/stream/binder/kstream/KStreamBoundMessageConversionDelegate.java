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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * @author Soby Chacko
 */
public class KStreamBoundMessageConversionDelegate {

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final SendToDlqAndContinue sendToDlqAndContinue;

	private final KStreamBinderConfigurationProperties kStreamBinderConfigurationProperties;

	private final BoundedKStreamPropertyCache boundedKStreamPropertyCache;

	public KStreamBoundMessageConversionDelegate(CompositeMessageConverterFactory compositeMessageConverterFactory,
												SendToDlqAndContinue sendToDlqAndContinue,
												KStreamBinderConfigurationProperties kStreamBinderConfigurationProperties,
												BoundedKStreamPropertyCache boundedKStreamPropertyCache) {
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
		this.sendToDlqAndContinue = sendToDlqAndContinue;
		this.kStreamBinderConfigurationProperties = kStreamBinderConfigurationProperties;
		this.boundedKStreamPropertyCache = boundedKStreamPropertyCache;
	}

	public KStream serializeOnOutbound(KStream<?,?> outboundBindTarget) {
		String contentType = this.boundedKStreamPropertyCache.getContentType(outboundBindTarget);
		MessageConverter messageConverter = StringUtils.hasText(contentType) ? compositeMessageConverterFactory
				.getMessageConverterForType(MimeType.valueOf(contentType))
				: null;

		return outboundBindTarget.map((k, v) -> {
			Message<?> message = v instanceof Message<?> ? (Message<?>) v :
					MessageBuilder.withPayload(v).build();
			Map<String, Object> headers = new HashMap<>(message.getHeaders());
			if (!StringUtils.isEmpty(contentType)) {
				headers.put(MessageHeaders.CONTENT_TYPE, contentType);
			}
			MessageHeaders messageHeaders = new MessageHeaders(headers);
			return new KeyValue<>(k,
					messageConverter.toMessage(message.getPayload(),
							messageHeaders).getPayload());
		});
	}

	private boolean equalTypeAndSubType(MimeType m1, MimeType m2) {
		return m1 != null && m2 != null && m1.getType().equalsIgnoreCase(m2.getType()) && m1.getSubtype().equalsIgnoreCase(m2.getSubtype());
	}

	@SuppressWarnings("unchecked")
	public KStream deserializeOnInbound(Class<?> valueClass, KStream<?, ?> bindingTarget) {
		MessageConverter messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();

		final AtomicReference<KeyValue<Object, Object>> keyValueAtomic = new AtomicReference<>();

		KStream<?, ?>[] branch = bindingTarget.branch(
			(o, o2) -> {
				boolean isValidRecord = false;

				try {
					if (valueClass.isAssignableFrom(o2.getClass())) {
						keyValueAtomic.set(new KeyValue<>(o, o2));
					}
					else if (o2 instanceof Message) {
						Object payload = ((Message) o2).getPayload();
						Object fx = ((Message) o2).getHeaders().get(MessageHeaders.CONTENT_TYPE);
						MimeType contentTypeToUse = fx instanceof String ? MimeType.valueOf((String) fx) : (MimeType) fx;
						if (payload instanceof byte[] && ("text".equalsIgnoreCase(contentTypeToUse.getType()) ||
								equalTypeAndSubType(MimeTypeUtils.APPLICATION_JSON, contentTypeToUse))) {
							payload = new String((byte[]) payload, StandardCharsets.UTF_8);
							Message<String> msg = MessageBuilder.withPayload((String) payload).copyHeaders(((Message) o2).getHeaders()).build();
							Object o1 = messageConverter.fromMessage(msg, valueClass);
							if (o1 == null) {
								throw new IllegalStateException("Inbound data conversion failed.");
							}

							keyValueAtomic.set(new KeyValue<>(o, o1));
						}
						else if (valueClass.isAssignableFrom(((Message) o2).getPayload().getClass())) {
							keyValueAtomic.set(new KeyValue<>(o, ((Message) o2).getPayload()));
						}
						else {
							Object o1 = messageConverter.fromMessage((Message) o2, valueClass);
							if (o1 == null) {
								throw new IllegalStateException("Inbound data conversion failed.");
							}
							keyValueAtomic.set(new KeyValue<>(o, o1));
						}
					}
					else if (o2 instanceof String || o2 instanceof byte[]) {
						Message<?> message = MessageBuilder.withPayload(o2).build();
						Object o1 = messageConverter.fromMessage(message, valueClass);
						if (o1 == null) {
							throw new IllegalStateException("Inbound data conversion failed.");
						}
						keyValueAtomic.set(new KeyValue<>(o, o1));
					}
					else {
						keyValueAtomic.set(new KeyValue<>(o, o2));
					}
					isValidRecord = true;
				}
				catch (Exception ignored) {
					//pass through
				}
				return isValidRecord;
			},
			(k, v) -> true
		);
		branch[1].process(() -> new Processor() {
			ProcessorContext context;

			@Override
			public void init(ProcessorContext context) {
				this.context = context;
			}

			@Override
			public void process(Object o, Object o2) {
				if (boundedKStreamPropertyCache.isEnableDlq(bindingTarget)) {
					String destination = boundedKStreamPropertyCache.getDestination(bindingTarget);
					if (o2 instanceof Message) {
						sendToDlqAndContinue.sendToDlq(destination, (byte[]) o, (byte[]) ((Message) o2).getPayload(), context.partition(), null);
					}
					else {
						sendToDlqAndContinue.sendToDlq(destination, (byte[]) o, (byte[]) o2, context.partition(), null);
					}
				}
				else if (kStreamBinderConfigurationProperties.getOnDeserializationError().isLogAndFail()) {
					throw new RuntimeException("Inbound deserialization failed.");
				}
				else {
					//quietly pass through. No action needed, this is akin to log and continue.
				}
			}

			@Override
			public void punctuate(long timestamp) {

			}

			@Override
			public void close() {

			}
		});

		return branch[0].map((o, o2) -> keyValueAtomic.get());
	}
}
