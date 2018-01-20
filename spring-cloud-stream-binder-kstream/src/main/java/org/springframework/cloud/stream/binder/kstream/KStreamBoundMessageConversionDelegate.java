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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * Delegate for handling all framework level message conversions inbound and outbound on {@link KStream}.
 * If native encoding is not enabled, then serialization will be performed on outbound messages based
 * on a contentType. Similarly, if native decoding is not enabled, deserialization will be performed on
 * inbound messages based on a contentType. Based on the contentType, a {@link MessageConverter} will
 * be resolved.
 *
 * @since 2.0.0
 *
 * @author Soby Chacko
 */
public class KStreamBoundMessageConversionDelegate {

	private static final ThreadLocal<KeyValue<Object, Object>> keyValueThreadLocal = new ThreadLocal<>();

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private final SendToDlqAndContinue sendToDlqAndContinue;

	private final KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue;

	public KStreamBoundMessageConversionDelegate(CompositeMessageConverterFactory compositeMessageConverterFactory,
												SendToDlqAndContinue sendToDlqAndContinue,
												KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue) {
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
		this.sendToDlqAndContinue = sendToDlqAndContinue;
		this.KStreamBindingInformationCatalogue = KStreamBindingInformationCatalogue;
	}

	/**
	 * Serialize {@link KStream} records on outbound based on contentType.
	 *
	 * @param outboundBindTarget outbound KStream target
	 * @return serialized KStream
	 */
	public KStream serializeOnOutbound(KStream<?,?> outboundBindTarget) {
		String contentType = this.KStreamBindingInformationCatalogue.getContentType(outboundBindTarget);
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

	/**
	 * Deserialize incoming {@link KStream} based on contentType.
	 *
	 * @param valueClass on KStream value
	 * @param bindingTarget inbound KStream target
	 * @return deserialized KStream
	 */
	@SuppressWarnings("unchecked")
	public KStream deserializeOnInbound(Class<?> valueClass, KStream<?, ?> bindingTarget) {
		MessageConverter messageConverter = compositeMessageConverterFactory.getMessageConverterForAllRegistered();

		KStream<?, ?>[] branch = bindingTarget.branch(
			(o, o2) -> {
				boolean isValidRecord = false;

				try {
					if (valueClass.isAssignableFrom(o2.getClass())) {
						keyValueThreadLocal.set(new KeyValue<>(o, o2));
					}
					else if (o2 instanceof Message) {
						if (valueClass.isAssignableFrom(((Message) o2).getPayload().getClass())) {
							keyValueThreadLocal.set(new KeyValue<>(o, ((Message) o2).getPayload()));
						}
						else {
							convertAndSetMessage(o, valueClass, messageConverter, (Message) o2);
						}
					}
					else if (o2 instanceof String || o2 instanceof byte[]) {
						Message<?> message = MessageBuilder.withPayload(o2).build();
						convertAndSetMessage(o, valueClass, messageConverter, message);
					}
					else {
						keyValueThreadLocal.set(new KeyValue<>(o, o2));
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
		processErrorFromDeserialization(bindingTarget, branch[1]);

		return branch[0].map((o, o2) -> {
			KeyValue<Object, Object> objectObjectKeyValue = keyValueThreadLocal.get();
			keyValueThreadLocal.remove();
			return objectObjectKeyValue;
		});
	}

	private void convertAndSetMessage(Object o, Class<?> valueClass, MessageConverter messageConverter, Message<?> msg) {
		Object messageConverted = messageConverter.fromMessage(msg, valueClass);
		if (messageConverted == null) {
			throw new IllegalStateException("Inbound data conversion failed.");
		}
		keyValueThreadLocal.set(new KeyValue<>(o, messageConverted));
	}

	@SuppressWarnings("unchecked")
	private void processErrorFromDeserialization(KStream<?, ?> bindingTarget, KStream<?, ?> branch) {
		branch.process(() -> new Processor() {
			ProcessorContext context;

			@Override
			public void init(ProcessorContext context) {
				this.context = context;
			}

			@Override
			public void process(Object o, Object o2) {
				if (KStreamBindingInformationCatalogue.isEnableDlq(bindingTarget)) {
					String destination = KStreamBindingInformationCatalogue.getDestination(bindingTarget);
					if (o2 instanceof Message) {
						Message message = (Message) o2;
						sendToDlqAndContinue.sendToDlq(destination, (byte[]) o, (byte[]) message.getPayload(), context.partition());
					}
					else {
						sendToDlqAndContinue.sendToDlq(destination, (byte[]) o, (byte[]) o2, context.partition());
					}
				}
				else if (KStreamBindingInformationCatalogue.getSerdeError(bindingTarget) == KStreamConsumerProperties.SerdeError.logAndFail) {
					throw new IllegalStateException("Inbound deserialization failed.");
				}
				else if (KStreamBindingInformationCatalogue.getSerdeError(bindingTarget) == KStreamConsumerProperties.SerdeError.logAndContinue) {
					//quietly pass through. No action needed, this is similar to log and continue.
				}
			}

			@Override
			public void punctuate(long timestamp) {

			}

			@Override
			public void close() {

			}
		});
	}
}
