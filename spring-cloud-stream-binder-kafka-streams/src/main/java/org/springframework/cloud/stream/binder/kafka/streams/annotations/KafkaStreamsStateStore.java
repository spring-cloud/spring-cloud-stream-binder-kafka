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

package org.springframework.cloud.stream.binder.kafka.streams.annotations;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)

public @interface KafkaStreamsStateStore {
	String name() default "";

	KafkaStreamsStateStoreProperties.StoreType type() default KafkaStreamsStateStoreProperties.StoreType.KEYVALUE;

	String keySerde() default "org.apache.kafka.common.serialization.Serdes$StringSerde";

	String valueSerde() default "org.apache.kafka.common.serialization.Serdes$StringSerde";

	long lengthMs() default 0;

	long retentionMs() default 0;

	boolean cache() default false;

	boolean logging() default true;
}
