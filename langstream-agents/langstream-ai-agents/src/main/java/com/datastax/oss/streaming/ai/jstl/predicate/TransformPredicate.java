/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.streaming.ai.jstl.predicate;

import com.datastax.oss.streaming.ai.TransformContext;
import java.util.function.Predicate;

/**
 * A predicate functional interface that applies to {@link TransformContext}. Implementations of
 * this interface should respect the current record encapsulated in the {@link TransformContext}
 * when evaluating the predicate.
 */
@FunctionalInterface
public interface TransformPredicate extends Predicate<TransformContext> {}
