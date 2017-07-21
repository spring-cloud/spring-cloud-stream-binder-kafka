package org.springframework.cloud.stream.binder.kstream.config;

public class KStreamBindingProperties {

	private KStreamConsumerProperties consumer = new KStreamConsumerProperties();

	private KStreamProducerProperties producer = new KStreamProducerProperties();

	public KStreamConsumerProperties getConsumer() {
		return consumer;
	}

	public void setConsumer(KStreamConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public KStreamProducerProperties getProducer() {
		return producer;
	}

	public void setProducer(KStreamProducerProperties producer) {
		this.producer = producer;
	}
}
