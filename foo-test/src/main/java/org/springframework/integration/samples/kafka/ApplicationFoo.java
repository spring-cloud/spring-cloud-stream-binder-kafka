package org.springframework.integration.samples.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;


/**
 * @author Soby Chacko
 */
@SpringBootApplication
public class ApplicationFoo {

	@Value("${kafka.topic}")
	private String topic;

	@Value("${kafka.messageKey}")
	private String messageKey;

	@Value("${kafka.broker.address}")
	private String brokerAddress;

	@Value("${kafka.zookeeper.connect}")
	private String zookeeperConnect;

	@Autowired
	ApplicationContext context;

	private static Binding<MessageChannel> bindingConsumer;

	public static void main(String[] args) throws Exception {
		System.out.println("hello world!!!");
		ConfigurableApplicationContext context
				= new SpringApplicationBuilder(ApplicationFoo.class)
				.web(false)
				.run(args);
		MessageChannel toKafka = context.getBean("toKafka", MessageChannel.class);
		for (int i = 0; i < 100 ; i++) {
			toKafka.send(new GenericMessage<>("foo-sobychacko" + i));
		}
		PollableChannel fromKafka = context.getBean("received", PollableChannel.class);
		Message<?> received = fromKafka.receive(10000);
		while (received != null) {
			System.out.println("foox:  " + received);
			received = fromKafka.receive(10000);
		}

		bindingConsumer.unbind();
		context.close();
		System.exit(0);
	}


	private KafkaBinderConfigurationProperties createConfigurationProperties() {
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		binderConfiguration.setBrokers(this.brokerAddress);
		binderConfiguration.setZkNodes(this.zookeeperConnect);
		binderConfiguration.setConsumerGroup("foo-group");
		return binderConfiguration;
	}

	private ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new KafkaProducerProperties());
	}

	@Bean
	public Binder kafkaMessageChannelBinder() throws Exception {
		KafkaMessageChannelBinder binder =
				new KafkaMessageChannelBinder(createConfigurationProperties());
		binder.onInit();
		binder.setApplicationContext(context);
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = new ExtendedConsumerProperties<>(new KafkaConsumerProperties());
		//properties.setHeaderMode(HeaderMode.raw);

		binder.bindProducer("foobarxyz", toKafka(), createProducerProperties());
		bindingConsumer = binder.bindConsumer("foobarxyz", "foo-group", received(),
				properties);

		return binder;
	}

	@Bean
	public PollableChannel received() {
		return new QueueChannel();
	}

	@Bean
	public DirectChannel toKafka() {
		return new DirectChannel();
	}

	@Bean
	public TopicCreator topicCreator() {
		return new TopicCreator(this.topic, this.zookeeperConnect);
	}

	public static class TopicCreator implements SmartLifecycle {

		private final String topic;

		private final String zkConnect;

		private volatile boolean running;

		public TopicCreator(String topic, String zkConnect) {
			System.out.println("setting topic: " + topic);
			this.topic = topic;
			this.zkConnect = zkConnect;
		}

		@Override
		public void start() {
			ZkUtils zkUtils = new ZkUtils(new ZkClient(this.zkConnect, 6000, 6000,
					ZKStringSerializer$.MODULE$), null, false);
			try {
				System.out.println("creating topic: " + topic);
				AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties());
			}
			catch (TopicExistsException e) {
				// no-op
			}
			this.running = true;
		}

		@Override
		public void stop() {
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		public int getPhase() {
			return Integer.MIN_VALUE;
		}

		@Override
		public boolean isAutoStartup() {
			return true;
		}

		@Override
		public void stop(Runnable callback) {
			callback.run();
		}

	}

}
