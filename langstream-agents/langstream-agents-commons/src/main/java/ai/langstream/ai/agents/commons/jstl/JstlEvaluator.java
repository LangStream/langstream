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

import ai.langstream.ai.agents.commons.MutableRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.el.ELContext;
import jakarta.el.ExpressionFactory;
import jakarta.el.PropertyNotFoundException;
import jakarta.el.ValueExpression;
import java.io.IOException;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.el.ExpressionFactoryImpl;

public class JstlEvaluator<T> {

    private static final ExpressionFactory FACTORY = new ExpressionFactoryImpl();
    private final ValueExpression valueExpression;
    private final String expression;
    private final ELContext expressionContext;

    public JstlEvaluator(String expression, Class<? extends T> type) {
        this.expressionContext = new StandardContext(FACTORY);
        this.expression =
                expression.startsWith("${") && expression.endsWith("}")
                        ? expression.substring(2, expression.length() - 1)
                        : expression;
        registerFunctions();
        this.valueExpression = FACTORY.createValueExpression(expressionContext, expression, type);
    }

    @SneakyThrows
    private void registerFunctions() {
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "length", JstlFunctions.class.getMethod("length", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "toJson", JstlFunctions.class.getMethod("toJson", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn", "fromJson", JstlFunctions.class.getMethod("fromJson", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "split",
                        JstlFunctions.class.getMethod("split", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "unpack",
                        JstlFunctions.class.getMethod("unpack", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "uppercase",
                        JstlFunctions.class.getMethod("uppercase", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "lowercase",
                        JstlFunctions.class.getMethod("lowercase", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "contains",
                        JstlFunctions.class.getMethod("contains", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "trim", JstlFunctions.class.getMethod("trim", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn", "concat", JstlFunctions.class.getMethod("concat", Object[].class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "concat3",
                        JstlFunctions.class.getMethod(
                                "concat3", Object.class, Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "coalesce",
                        JstlFunctions.class.getMethod("coalesce", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "str", JstlFunctions.class.getMethod("toString", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn", "toString", JstlFunctions.class.getMethod("toString", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn", "toDouble", JstlFunctions.class.getMethod("toDouble", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "filter",
                        JstlFunctions.class.getMethod("filter", Object.class, String.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "addAll",
                        JstlFunctions.class.getMethod("addAll", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn", "listOf", JstlFunctions.class.getMethod("listOf", Object[].class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "emptyList", JstlFunctions.class.getMethod("emptyList"));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "mapToListOfStructs",
                        JstlFunctions.class.getMethod(
                                "mapToListOfStructs", Object.class, String.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "listToListOfStructs",
                        JstlFunctions.class.getMethod(
                                "listToListOfStructs", Object.class, String.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "listAdd",
                        JstlFunctions.class.getMethod("listAdd", Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "emptyMap", JstlFunctions.class.getMethod("emptyMap"));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "mapPut",
                        JstlFunctions.class.getMethod(
                                "mapPut", Object.class, Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "mapOf", JstlFunctions.class.getMethod("mapOf", Object[].class));

        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "mapRemove",
                        JstlFunctions.class.getMethod("mapRemove", Object.class, Object.class));

        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "toInt", JstlFunctions.class.getMethod("toInt", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "toListOfFloat",
                        JstlFunctions.class.getMethod("toListOfFloat", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "toLong", JstlFunctions.class.getMethod("toLong", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "replace",
                        JstlFunctions.class.getMethod(
                                "replace", Object.class, Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "now", JstlFunctions.class.getMethod("now"));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "uuid", JstlFunctions.class.getMethod("uuid"));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction("fn", "random", JstlFunctions.class.getMethod("random", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "timestampAdd",
                        JstlFunctions.class.getMethod(
                                "timestampAdd", Object.class, Object.class, Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "toSQLTimestamp",
                        JstlFunctions.class.getMethod("toSQLTimestamp", Object.class));
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "decimalFromUnscaled",
                        JstlFunctions.class.getMethod("toBigDecimal", Object.class, Object.class));

        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "decimalFromNumber",
                        JstlFunctions.class.getMethod("toBigDecimal", Object.class));

        // Deprecated
        this.expressionContext
                .getFunctionMapper()
                .mapFunction(
                        "fn",
                        "dateadd",
                        JstlFunctions.class.getMethod(
                                "dateadd", Object.class, Object.class, Object.class));
    }

    public T evaluate(MutableRecord mutableRecord) {
        JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(mutableRecord);
        FACTORY.createValueExpression(expressionContext, "${key}", Object.class)
                .setValue(expressionContext, adapter.getKey());
        FACTORY.createValueExpression(expressionContext, "${value}", Object.class)
                .setValue(expressionContext, adapter.adaptValue());

        // this is only for fn:filter
        FACTORY.createValueExpression(expressionContext, "${record}", Object.class)
                .setValue(expressionContext, adapter.adaptRecord());

        // Register message headers as top level fields
        FACTORY.createValueExpression(expressionContext, "${messageKey}", String.class)
                .setValue(expressionContext, adapter.getHeader().get("messageKey"));
        FACTORY.createValueExpression(expressionContext, "${topicName}", String.class)
                .setValue(expressionContext, adapter.getHeader().get("topicName"));
        FACTORY.createValueExpression(expressionContext, "${destinationTopic}", String.class)
                .setValue(expressionContext, adapter.getHeader().get("destinationTopic"));
        FACTORY.createValueExpression(expressionContext, "${eventTime}", Long.class)
                .setValue(expressionContext, adapter.getHeader().get("eventTime"));
        FACTORY.createValueExpression(expressionContext, "${properties}", Map.class)
                .setValue(expressionContext, adapter.getHeader().get("properties"));
        try {
            return this.valueExpression.getValue(expressionContext);
        } catch (PropertyNotFoundException notFound) {

            // this is a very common error, so we provide a better error message
            if (expression.startsWith("value.")) {
                Object valueObject = mutableRecord.getValueObject();
                if (valueObject instanceof String s) {
                    try {
                        new ObjectMapper().readValue(s, Object.class);
                    } catch (IOException error) {
                        throw new IllegalArgumentException(
                                "The property referred by "
                                        + expression
                                        + " couldn't be found, "
                                        + "this the error from a JSON parser: \n"
                                        + error.getMessage(),
                                notFound);
                    }
                }
            }
            throw new IllegalArgumentException(notFound);
        }
    }

    public T evaluateRawContext(Map<String, Object> context) {
        for (Map.Entry<String, Object> stringObjectEntry : context.entrySet()) {
            FACTORY.createValueExpression(
                            expressionContext,
                            "${%s}".formatted(stringObjectEntry.getKey()),
                            Object.class)
                    .setValue(expressionContext, stringObjectEntry.getValue());
        }
        return this.valueExpression.getValue(expressionContext);
    }

    public String toString() {
        return "JstlEvaluator(expression=" + this.expression + ")";
    }
}
