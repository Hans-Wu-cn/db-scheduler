/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.solon;

import java.util.concurrent.atomic.AtomicLong;
import org.noear.solon.annotation.Component;

@Component
public class CounterService {
  private final AtomicLong count = new AtomicLong(0L);

  public void increase() {
    count.incrementAndGet();
  }

  public long read() {
    return count.get();
  }
}