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

package org.springframework.cloud.stream.binder.kstream.config;

import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;

/**
 * @author Soby Chacko
 */
public class KStreamBinderConfigurationProperties extends KafkaBinderConfigurationProperties {

	private OnDeserializationError onDeserializationError = new OnDeserializationError();

	public OnDeserializationError getOnDeserializationError() {
		return onDeserializationError;
	}

	public void setOnDeserializationError(OnDeserializationError onDeserializationError) {
		this.onDeserializationError = onDeserializationError;
	}

	public static class OnDeserializationError {

		boolean logAndContinue = false;
		boolean logAndFail = false;
		boolean sendToDlq = false;

		public boolean isLogAndContinue() {
			return logAndContinue;
		}

		public void setLogAndContinue(boolean logAndContinue) {
			this.logAndContinue = logAndContinue;
		}

		public boolean isLogAndFail() {
			return logAndFail;
		}

		public void setLogAndFail(boolean logAndFail) {
			this.logAndFail = logAndFail;
		}

		public boolean isSendToDlq() {
			return sendToDlq;
		}

		public void setSendToDlq(boolean sendToDlq) {
			this.sendToDlq = sendToDlq;
		}
	}
}
