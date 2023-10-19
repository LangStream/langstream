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
package ai.langstream.cli;

public interface CLILogger {
    void log(Object message);

    void error(Object message);

    boolean isDebugEnabled();

    void debug(Object message);

    class SystemCliLogger implements CLILogger {
        @Override
        public void log(Object message) {
            System.out.println(message);
        }

        @Override
        public void error(Object message) {
            System.err.println(message);
        }

        @Override
        public boolean isDebugEnabled() {
            return true;
        }

        @Override
        public void debug(Object message) {
            log(message);
        }
    }
}
