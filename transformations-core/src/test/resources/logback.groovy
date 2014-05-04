/*
 * Copyright (c) 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// We highly recommended that you always add a status listener just
// after the last import statement and before all other statements
statusListener(OnConsoleStatusListener)

scan()

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d [%8.8thread] [%-5level] [%logger{0}] %msg%n"
    }
}
root(INFO, ["STDOUT"])
