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

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.StringConvertingContentTypeResolver;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class KStreamBoundElementFactory extends AbstractBindingTargetFactory<KStream> {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final KStreamBuilder kStreamBuilder;

	private final BindingServiceProperties bindingServiceProperties;

	private volatile Codec codec;

	private final StringConvertingContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private volatile Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	private CompositeMessageConverterFactory compositeMessageConverterFactory;

	public KStreamBoundElementFactory(KStreamBuilder streamBuilder, BindingServiceProperties bindingServiceProperties,
			Codec codec, CompositeMessageConverterFactory compositeMessageConverterFactory) {
		super(KStream.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kStreamBuilder = streamBuilder;
		this.codec = codec;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
	}

	@Override
	public KStream createInput(String name) {
		KStream<Object, Object> stream = kStreamBuilder.stream(bindingServiceProperties.getBindingDestination(name));
		ConsumerProperties properties = bindingServiceProperties.getConsumerProperties(name);
		if (HeaderMode.embeddedHeaders.equals(properties.getHeaderMode())) {

			stream = stream.map(new KeyValueMapper<Object, Object, KeyValue<Object, Object>>() {
				@Override
				public KeyValue<Object, Object> apply(Object key, Object value) {
					if (!(value instanceof byte[])) {
						return new KeyValue<>(key, value);
					}
					try {
						MessageValues messageValues = EmbeddedHeaderUtils
								.extractHeaders(MessageBuilder.withPayload((byte[]) value).build(), true);
						messageValues = deserializePayloadIfNecessary(messageValues);
						return new KeyValue<Object, Object>(null, messageValues.toMessage());
					}
					catch (Exception e) {
						throw new IllegalArgumentException(e);
					}
				}
			});
		}
		return stream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream createOutput(final String name) {
		return new KStreamDelegate() {
			@Override
			public void setDelegate(KStream delegate) {
				BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
				if (StringUtils.hasText(bindingProperties.getContentType())) {
					final MessageConverter messageConverter = compositeMessageConverterFactory
							.getMessageConverterForType(MimeType.valueOf(bindingProperties.getContentType()));

					delegate = delegate.map(new KeyValueMapper<Object, Object, KeyValue<Object, ? extends Message<?>>>() {
						@Override
						public KeyValue<Object, ? extends Message<?>> apply(Object k, Object v) {
							Message<?> message = (Message<?>) v;
							return new KeyValue<Object, Message<?>>(k, messageConverter.toMessage(message.getPayload(),
									new MutableMessageHeaders(((Message<?>) v).getHeaders())));

						}
					});
				}
				super.setDelegate(delegate);
			}
		};
	}

	private MessageValues deserializePayloadIfNecessary(MessageValues messageValues) {
		Object originalPayload = messageValues.getPayload();
		MimeType contentType = this.contentTypeResolver.resolve(messageValues);
		Object payload = deserializePayload(originalPayload, contentType);
		if (payload != null) {
			messageValues.setPayload(payload);
			Object originalContentType = messageValues.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			// Reset content-type only if the original content type is not null (when
			// receiving messages from
			// non-SCSt applications).
			if (originalContentType != null) {
				messageValues.put(MessageHeaders.CONTENT_TYPE, originalContentType);
				messageValues.remove(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			}
		}
		return messageValues;
	}

	private Object deserializePayload(Object payload, MimeType contentType) {
		if (payload instanceof byte[]) {
			if (contentType == null || MimeTypeUtils.APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			} else {
				return deserializePayload((byte[]) payload, contentType);
			}
		}
		return payload;
	}

	private Object deserializePayload(byte[] bytes, MimeType contentType) {
		if ("text".equalsIgnoreCase(contentType.getType()) || MimeTypeUtils.APPLICATION_JSON.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				String errorMessage = "unable to deserialize [java.lang.String]. Encoding not supported. " + e.getMessage();
				logger.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		} else {
			String className = JavaClassMimeTypeConversion.classNameFromMimeType(contentType);
			try {
				// Cache types to avoid unnecessary ClassUtils.forName calls.
				Class<?> targetType = this.payloadTypeCache.get(className);
				if (targetType == null) {
					assert className != null;
					targetType = ClassUtils.forName(className, null);
					this.payloadTypeCache.put(className, targetType);
				}
				return this.codec.decode(bytes, targetType);
			}// catch all exceptions that could occur during de-serialization
			catch (Exception e) {
				String errorMessage = "Unable to deserialize [" + className + "] using the contentType [" + contentType + "] " + e.getMessage();
				logger.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		}
	}

	abstract static class JavaClassMimeTypeConversion {

		static String classNameFromMimeType(MimeType mimeType) {
			Assert.notNull(mimeType, "mimeType cannot be null.");
			String className = mimeType.getParameter("type");
			if (className == null) {
				return null;
			}
			// unwrap quotes if any
			className = className.replace("\"", "");

			// restore trailing ';'
			if (className.contains("[L")) {
				className += ";";
			}
			return className;
		}

	}
}
