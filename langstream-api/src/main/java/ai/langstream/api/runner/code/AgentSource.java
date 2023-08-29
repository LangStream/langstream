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
package ai.langstream.api.runner.code;

import ai.langstream.api.runtime.ComponentType;
import java.util.List;

/** Body of the agent */
public interface AgentSource extends AgentCode {

    /**
     * The agent generates records returns them as list of records.
     *
     * @return the list of output records
     * @throws Exception if the agent fails to process the records
     */
    List<Record> read() throws Exception;

    /**
     * Called by the framework to indicate that the agent has successfully processed all the records
     * returned by read up to the latest.
     */
    void commit(List<Record> records) throws Exception;

    @Override
    default ComponentType componentType() {
        return ComponentType.SOURCE;
    }

    /**
     * Called by the framework to indicate that the agent has failed to process permanently the
     * records returned by read up to the latest. For instance the source may send the records to a
     * dead letter queue or throw an error
     *
     * @param record the record that failed
     * @throws Exception if the source fails to process the permanently failed record
     */
    default void permanentFailure(Record record, Exception error) throws Exception {
        throw error;
    }
}
