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
package ai.langstream.ai.agents.commons.jstl;

import jakarta.el.BeanELResolver;
import jakarta.el.ELContext;
import jakarta.el.MethodNotFoundException;

/**
 * The purpose of this BeanELResolver is to disable methods invocations. By default, the EL
 * implementation allows for invoking JDK methods on beans like ${value.toString()}. We prefer to
 * disable such invocation for two reasons:
 *
 * <ul>
 *   <li>Have more controls over the transformations API and provide utility methods instead under
 *       the "fn:" prefix. Otherwise, it hard to guarantee the availability of the API as the JDK
 *       evolves.
 *   <li>Security reasons as users may get access to internal methods that have unintended side
 *       effects.
 * </ul>
 */
public class DisabledInvocationBeanResolver extends BeanELResolver {
    @Override
    public Object invoke(
            ELContext context, Object base, Object method, Class<?>[] paramTypes, Object[] params) {
        throw new MethodNotFoundException("Method invocations are disabled: " + method);
    }
}
