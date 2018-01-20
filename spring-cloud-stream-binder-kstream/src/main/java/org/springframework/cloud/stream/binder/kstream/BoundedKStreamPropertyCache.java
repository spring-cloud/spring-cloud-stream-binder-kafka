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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;

/**
 * @author Soby Chacko
 */
public class BoundedKStreamPropertyCache {

	private Map<KStream<?, ?>, BindingProperties> bindingTargetToBindingProperties = new ConcurrentHashMap<>();
	private Map<KStream<?, ?>, KStreamConsumerProperties> bindingTargetToConsumerProperties = new ConcurrentHashMap<>();

	public String getDestination(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = bindingTargetToBindingProperties.get(bindingTarget);
		return bindingProperties.getDestination();
	}

	public boolean isUseNativeDecoding(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = bindingTargetToBindingProperties.get(bindingTarget);
		return bindingProperties.getConsumer().isUseNativeDecoding();
	}

	public boolean isEnableDlq(KStream<?,?> bindingTarget) {
		return bindingTargetToConsumerProperties.get(bindingTarget).isEnableDlq();
	}

	public String getContentType(KStream<?,?> bindingTarget) {
		BindingProperties bindingProperties = bindingTargetToBindingProperties.get(bindingTarget);
		return bindingProperties.getContentType();
	}

	public void addBindingTargetToBindingProperties(KStream<?,?> bindingTarget, BindingProperties bindingProperties) {
		this.bindingTargetToBindingProperties.put(bindingTarget, bindingProperties);
	}

	public void addBindingTargetToConsumerProperties(KStream<?,?> bindingTarget, KStreamConsumerProperties kStreamConsumerProperties) {
		this.bindingTargetToConsumerProperties.put(bindingTarget, kStreamConsumerProperties);
	}

}
