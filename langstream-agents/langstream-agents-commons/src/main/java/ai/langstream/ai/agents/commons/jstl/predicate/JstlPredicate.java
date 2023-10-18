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
package ai.langstream.ai.agents.commons.jstl.predicate;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import jakarta.el.ELException;
import jakarta.el.PropertyNotFoundException;
import lombok.extern.slf4j.Slf4j;

/** A {@link TransformPredicate} implementation based on the Uniform Transform Language. */
@Slf4j
public class JstlPredicate implements TransformPredicate {
    private final JstlEvaluator<Boolean> evaluator;

    public JstlPredicate(String when) {
        try {
            final String expression = String.format("${%s}", when);
            this.evaluator = new JstlEvaluator<>(expression, boolean.class);
        } catch (ELException ex) {
            throw new IllegalArgumentException("invalid when: " + when, ex);
        }
    }

    @Override
    public boolean test(MutableRecord mutableRecord) {
        try {
            return this.evaluator.evaluate(mutableRecord);
        } catch (PropertyNotFoundException ex) {
            log.warn("a property in the when expression was not found in the message", ex);
            return false;
        } catch (IllegalArgumentException ex) {
            if (ex.getCause() instanceof PropertyNotFoundException) {
                log.warn(
                        "a property in the when expression was not found in the message",
                        ex.getCause());
                return false;
            } else {
                throw ex;
            }
        }
    }
}
