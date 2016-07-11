/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.zookeeper.ZKUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaProducerProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.cluster.Partition;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderTests extends
		PartitionCapableBinderTests<KafkaTestBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	private static long uniqueBindingId = System.currentTimeMillis();

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10, "retryTest." + uniqueBindingId + ".0",
			"error.retryTest." + uniqueBindingId + ".0.testGroup");

	private KafkaTestBinder binder;

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected KafkaTestBinder getBinder() {
		if (binder == null) {
			KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
			binder = new KafkaTestBinder(binderConfiguration);
		}
		return binder;
	}

	private KafkaBinderConfigurationProperties createConfigurationProperties() {
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		BrokerAddress[] brokerAddresses = embeddedKafka.getBrokerAddresses();
		List<String> bAddresses = new ArrayList<>();
		for (BrokerAddress bAddress : brokerAddresses) {
			bAddresses.add(bAddress.toString());
		}
		String[] foo = new String[bAddresses.size()];
		binderConfiguration.setBrokers(bAddresses.toArray(foo));
		binderConfiguration.setZkNodes(embeddedKafka.getZookeeperConnectionString());
		//binderConfiguration.setConsumerGroup("testGroup");
		return binderConfiguration;
	}

	private KafkaBinderConfigurationProperties createConfigurationProperties1() {
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		BrokerAddress[] ba = embeddedKafka.getBrokerAddresses();
		List<String> baddresses = new ArrayList<>();
		for (BrokerAddress badd : ba) {
			baddresses.add(badd.toString());
		}
		String[] foo = new String[baddresses.size()];
		binderConfiguration.setBrokers(baddresses.toArray(foo));
		binderConfiguration.setZkNodes(embeddedKafka.getZookeeperConnectionString());
		binderConfiguration.setConsumerGroup("startOffsets");
		return binderConfiguration;
	}


	@Override
	protected ExtendedConsumerProperties<KafkaConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new KafkaConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new KafkaProducerProperties());
	}

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}
	}

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(final String name) {
		throw new UnsupportedOperationException("'spyOn' is not used by Kafka tests");
	}


	private ConsumerFactory<byte[], byte[]> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getKafkaConnectionString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, configurationProperties.getConsumerGroup());
		Deserializer<byte[]> valueDecoder = new ByteArrayDeserializer();
		Deserializer<byte[]> keyDecoder = new ByteArrayDeserializer();

		return new DefaultKafkaConsumerFactory<>(props, keyDecoder, valueDecoder);

	}

	@Test
	@Ignore
	public void testDlqAndRetry() {
		KafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		QueueChannel dlqChannel = new QueueChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);

		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		Message<?> receivedMessage = receive(dlqChannel, 3);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	public void testDefaultAutoCommitOnErrorWithoutDlq() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		//long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		assertThat(handler.getLatch().await((int) (timeoutMultiplier * 1000), TimeUnit.MILLISECONDS));
		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> receivedMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		consumerBinding.unbind();

		// on the second attempt the message is redelivered
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage2 = MessageBuilder.withPayload(testMessage2Payload).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> firstReceived = receive(successfulInputChannel);
		assertThat(firstReceived.getPayload()).isEqualTo(testMessagePayload);
		Message<?> secondReceived = receive(successfulInputChannel);
		assertThat(secondReceived.getPayload()).isEqualTo(testMessage2Payload);
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@Ignore
	public void testDefaultAutoCommitOnErrorWithDlq() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		DirectChannel moduleInputChannel = new DirectChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);
		QueueChannel dlqChannel = new QueueChannel();
		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage = MessageBuilder.withPayload(testMessagePayload).build();
		moduleOutputChannel.send(testMessage);

		Message<?> dlqMessage = receive(dlqChannel, 3);
		assertThat(dlqMessage).isNotNull();
		assertThat(dlqMessage.getPayload()).isEqualTo(testMessagePayload);

		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> handledMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(handledMessage).isNotNull();
		assertThat(handledMessage.getPayload()).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());

		dlqConsumerBinding.unbind();
		consumerBinding.unbind();

		// on the second attempt the message is not redelivered because the DLQ is set
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<String> testMessage2 = MessageBuilder.withPayload(testMessage2Payload).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> receivedMessage = receive(successfulInputChannel);
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessage2Payload);

		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateKafkaTopicName() {
		KafkaMessageChannelBinder.validateTopicName("foo:bar");
	}

	@Test
	public void testCompression() throws Exception {
		final KafkaProducerProperties.CompressionType[] codecs = new KafkaProducerProperties.CompressionType[] {
				KafkaProducerProperties.CompressionType.none, KafkaProducerProperties.CompressionType.gzip,
				KafkaProducerProperties.CompressionType.snappy };
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaTestBinder binder = getBinder();
		for (KafkaProducerProperties.CompressionType codec : codecs) {
			DirectChannel moduleOutputChannel = new DirectChannel();
			QueueChannel moduleInputChannel = new QueueChannel();
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.getExtension().setCompressionType(KafkaProducerProperties.CompressionType.valueOf(codec.toString()));
			Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel,
					producerProperties);
			Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel,
					createConsumerProperties());
			Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
					.build();
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(moduleInputChannel);
			assertThat(inbound).isNotNull();
			assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@Test
	public void testCustomPartitionCountOverridesDefaultIfLarger() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(10);
		KafkaTestBinder binder = new KafkaTestBinder(binderConfiguration);

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);


		Collection<PartitionInfo> partitions =
				consumerFactory().createConsumer().partitionsFor("foo" + uniqueBindingId + ".0");

		assertThat(partitions).hasSize(10);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountDoesNotOverridePartitioningIfSmaller() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(6);
		KafkaTestBinder binder = new KafkaTestBinder(binderConfiguration);
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
		Collection<PartitionInfo> partitions =
				consumerFactory().createConsumer().partitionsFor("foo" + uniqueBindingId + ".0");

		assertThat(partitions).hasSize(6);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testCustomPartitionCountOverridesPartitioningIfLarger() throws Exception {

		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(4);
		KafkaTestBinder binder = new KafkaTestBinder(binderConfiguration);

		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
		Collection<PartitionInfo> partitions =
				consumerFactory().createConsumer().partitionsFor("foo" + uniqueBindingId + ".0");
		assertThat(partitions).hasSize(5);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultConsumerStartsAtEarliest() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(createConfigurationProperties1());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		binder.bindConsumer(testTopicName, "startOffsets", input1, createConsumerProperties());
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEarliest() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		binder.bindProducer(testTopicName, output, createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
		binder.bindConsumer(testTopicName, "startOffsets", input1, properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);
	}

	@Test
	@Ignore
	@SuppressWarnings("unchecked")
	public void testReset() throws Exception {
		KafkaTestBinder binder = getBinder();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();

		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.getExtension().setResetOffsets(true);
		properties.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
				properties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);
		consumerBinding.unbind();

		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		ExtendedConsumerProperties<KafkaConsumerProperties> properties2 = createConsumerProperties();
		properties2.getExtension().setResetOffsets(true);
		properties2.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
		consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, properties2);
		Message<byte[]> receivedMessage4 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage4).isNotNull();
		assertThat(new String(receivedMessage4.getPayload())).isEqualTo(testPayload1);
		Message<byte[]> receivedMessage5 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage5).isNotNull();
		assertThat(new String(receivedMessage5.getPayload())).isEqualTo(testPayload2);
		Message<byte[]> receivedMessage6 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage6).isNotNull();
		assertThat(new String(receivedMessage6.getPayload())).isEqualTo(testPayload3);
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@Ignore
	@SuppressWarnings("unchecked")
	public void testResume() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		QueueChannel input1 = new QueueChannel();

		String testTopicName = UUID.randomUUID().toString();
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				createProducerProperties());
		String testPayload1 = "foo1-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));
		ExtendedConsumerProperties<KafkaConsumerProperties> firstConsumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
				firstConsumerProperties);
		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		String testPayload2 = "foo2-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isNotNull();
		consumerBinding.unbind();

		String testPayload3 = "foo3-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, consumerProperties);
		Message<byte[]> receivedMessage3 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage3).isNotNull();
		assertThat(new String(receivedMessage3.getPayload())).isEqualTo(testPayload3);
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@Ignore
	public void testSyncProducerMetadata() throws Exception {
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(createConfigurationProperties());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		String testTopicName = UUID.randomUUID().toString();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.getExtension().setSync(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		DirectFieldAccessor accessor = new DirectFieldAccessor(extractEndpoint(producerBinding));
		MessageHandler handler = (MessageHandler) accessor.getPropertyValue("handler");
		DirectFieldAccessor accessor1 = new DirectFieldAccessor(handler);
//		ProducerConfiguration producerConfiguration = (ProducerConfiguration) accessor1
//				.getPropertyValue("producerConfiguration");
//		assertThat(producerConfiguration.getProducerMetadata().isSync())
//				.withFailMessage("Kafka Sync Producer should have been enabled.");
		producerBinding.unbind();
	}

	@Test
	public void testAutoCreateTopicsDisabledFailsIfTopicMissing() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(false);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting" + System.currentTimeMillis();
		try {
			binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
			fail();
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(BinderException.class);
			assertThat(e).hasMessageContaining("Topic " + testTopicName + " does not exist");
		}

//		try {
		//this call seems to create the topic
//			Collection<PartitionInfo> partitions =
//					consumerFactory().createConsumer().partitionsFor(testTopicName);
//			System.out.println("foobar" + partitions);
//			fail();
//		}
//		catch (Exception e) {
//			//assertThat(e).isInstanceOf(TopicNotFoundException.class);
//		}
	}

	@Test
	public void testAutoConfigureTopicsDisabledSucceedsIfTopicExisting() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		String testTopicName = "existing" + System.currentTimeMillis();
		AdminUtils.createTopic(zkUtils, testTopicName, 5, 1, new Properties());

		configurationProperties.setAutoCreateTopics(false);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	@Test
	public void testAutoAddPartitionsDisabledFailsIfTopicUnderpartitioned() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		String testTopicName = "existing" + System.currentTimeMillis();
		AdminUtils.createTopic(zkUtils, testTopicName, 1, 1, new Properties());
		configurationProperties.setAutoAddPartitions(false);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);
		try {
			binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(BinderException.class);
			assertThat(e)
					.hasMessageContaining("The number of expected partitions was: 3, but 1 has been found instead");
		}
	}

	@Test
	@Ignore
	public void testAutoAddPartitionsDisabledSucceedsIfTopicPartitionedCorrectly() throws Exception {

		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		String testTopicName = "existing" + System.currentTimeMillis();
		AdminUtils.createTopic(zkUtils, testTopicName, 6, 1, new Properties());
		configurationProperties.setAutoAddPartitions(false);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);

		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);

		Partition[] listenedPartitions = TestUtils.getPropertyValue(binding,
				"endpoint.val$messageListenerContainer.partitions", Partition[].class);

		assertThat(listenedPartitions).hasSize(2);
		assertThat(listenedPartitions).contains(new Partition(testTopicName, 2, null, null), new Partition(testTopicName, 5, null, null));
		Collection<PartitionInfo> partitions =
					consumerFactory().createConsumer().partitionsFor(testTopicName);
		assertThat(partitions).hasSize(6);
		binding.unbind();
	}

	@Test
	public void testAutoCreateTopicsEnabledSucceeds() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(true);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting" + System.currentTimeMillis();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
	}

	@Test
	public void testPartitionCountNotReduced() throws Exception {
		String testTopicName = "existing" + System.currentTimeMillis();

		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		AdminUtils.createTopic(zkUtils, testTopicName, 6, 1, new Properties());
		configurationProperties.setAutoAddPartitions(true);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
		TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(testTopicName,
				zkUtils);
		assertThat(topicMetadata.partitionsMetadata().size()).isEqualTo(6);
	}

	@Test
	public void testPartitionCountIncreasedIfAutoAddPartitionsSet() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);

		String testTopicName = "existing" + System.currentTimeMillis();
		AdminUtils.createTopic(zkUtils, testTopicName, 1, 1, new Properties());
		configurationProperties.setMinPartitionCount(6);
		configurationProperties.setAutoAddPartitions(true);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		binder.afterPropertiesSet();
		RetryTemplate metatadataRetrievalRetryOperations = new RetryTemplate();
		metatadataRetrievalRetryOperations.setRetryPolicy(new SimpleRetryPolicy());
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		metatadataRetrievalRetryOperations.setBackOffPolicy(backOffPolicy);
		binder.setMetadataRetryOperations(metatadataRetrievalRetryOperations);
		DirectChannel output = new DirectChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<?> binding = binder.doBindConsumer(testTopicName, "test", output, consumerProperties);
		binding.unbind();
		TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(testTopicName,
				zkUtils);
		assertThat(topicMetadata.partitionsMetadata().size()).isEqualTo(6);
	}

	private static final class FailingInvocationCountingMessageHandler implements MessageHandler {

		private int invocationCount;

		private final LinkedHashMap<Long, Message<?>> receivedMessages = new LinkedHashMap<>();

		private final CountDownLatch latch;

		private FailingInvocationCountingMessageHandler(int latchSize) {
			latch = new CountDownLatch(latchSize);
		}

		private FailingInvocationCountingMessageHandler() {
			this(1);
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			invocationCount++;
			Long offset = message.getHeaders().get(KafkaHeaders.OFFSET, Long.class);
			// using the offset as key allows to ensure that we don't store duplicate
			// messages on retry
			if (!receivedMessages.containsKey(offset)) {
				receivedMessages.put(offset, message);
				latch.countDown();
			}
			throw new RuntimeException();
		}

		public LinkedHashMap<Long, Message<?>> getReceivedMessages() {
			return receivedMessages;
		}

		public int getInvocationCount() {
			return invocationCount;
		}

		public CountDownLatch getLatch() {
			return latch;
		}
	}
}
