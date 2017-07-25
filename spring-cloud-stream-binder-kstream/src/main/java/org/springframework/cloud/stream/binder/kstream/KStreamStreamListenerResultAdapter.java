package org.springframework.cloud.stream.binder.kstream;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Marius Bogoevici
 */
public class KStreamStreamListenerResultAdapter implements StreamListenerResultAdapter<KStream, KStreamBoundElementFactory.KStreamWrapper> {

	@Override
	public boolean supports(Class<?> resultType, Class<?> boundElement) {
		return KStream.class.isAssignableFrom(resultType) && KStream.class.isAssignableFrom(boundElement);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Closeable adapt(KStream streamListenerResult, KStreamBoundElementFactory.KStreamWrapper boundElement) {
		boundElement.wrap(streamListenerResult.map(new KeyValueMapper() {
			@Override
			public Object apply(Object k, Object v) {
				if (v instanceof Message<?>) {
					return new KeyValue<>(k, v);
				}
				else {
					return new KeyValue<>(k, MessageBuilder.withPayload(v).build());
				}
			}
		}));
		return new NoOpCloseeable();
	}

	private static final class NoOpCloseeable implements Closeable {

		@Override
		public void close() throws IOException {

		}

	}
}
