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

import jakarta.el.ArrayELResolver;
import jakarta.el.BeanNameELResolver;
import jakarta.el.BeanNameResolver;
import jakarta.el.CompositeELResolver;
import jakarta.el.ELResolver;
import jakarta.el.ExpressionFactory;
import jakarta.el.ListELResolver;
import jakarta.el.MapELResolver;
import jakarta.el.PropertyNotWritableException;
import jakarta.el.ResourceBundleELResolver;
import jakarta.el.StandardELContext;
import jakarta.el.StaticFieldELResolver;
import java.util.HashMap;
import java.util.Map;

/**
 * A standard context that mirrors {@link jakarta.el.StandardELContext} with the exception that it
 * registers a custom beans resolver that disables invocations.
 */
public class StandardContext extends StandardELContext {
    private final CompositeELResolver standardResolver;

    public StandardContext(ExpressionFactory factory) {
        super(factory);
        this.standardResolver = new CompositeELResolver();
        ELResolver streamResolver = factory.getStreamELResolver();
        this.standardResolver.add(new BeanNameELResolver(new StandardBeanNameResolver()));
        if (streamResolver != null) {
            this.standardResolver.add(streamResolver);
        }
        this.standardResolver.add(JstlTypeConverter.INSTANCE);

        this.standardResolver.add(new StaticFieldELResolver());
        this.standardResolver.add(new MapELResolver());
        this.standardResolver.add(new ResourceBundleELResolver());
        this.standardResolver.add(new ListELResolver());
        this.standardResolver.add(new ArrayELResolver());
        this.standardResolver.add(new DisabledInvocationBeanResolver());
    }

    public ELResolver getELResolver() {
        return this.standardResolver;
    }

    private static class StandardBeanNameResolver extends BeanNameResolver {
        private final Map<String, Object> beans = new HashMap<>();

        public boolean isNameResolved(String beanName) {
            return this.beans.containsKey(beanName);
        }

        public Object getBean(String beanName) {
            return this.beans.get(beanName);
        }

        public void setBeanValue(String beanName, Object value)
                throws PropertyNotWritableException {
            this.beans.put(beanName, value);
        }

        public boolean isReadOnly(String beanName) {
            return false;
        }

        public boolean canCreateBean(String beanName) {
            return true;
        }
    }
}
