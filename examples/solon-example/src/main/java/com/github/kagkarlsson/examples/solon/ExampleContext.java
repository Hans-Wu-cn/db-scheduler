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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import java.time.Instant;
import java.util.function.Consumer;
import org.noear.solon.data.annotation.TranAnno;
import org.noear.solon.data.tran.TranUtils;
import org.noear.solon.data.tran.interceptor.TranInterceptor;
import org.slf4j.Logger;

public class ExampleContext {
  public SchedulerClient schedulerClient;
  private Logger logger;

  public ExampleContext(SchedulerClient schedulerClient, Logger logger) {
    this.schedulerClient = schedulerClient;
    this.logger = logger;
  }

  public void log(String message) {
    logger.info(message);
  }

  public void executeWithoutResult(){

  }
  public void executeWithoutResult(Consumer<Object> action) throws RuntimeException {
    try {
      TranUtils.execute(new TranAnno(), ()->{
        action.accept(new Object());
      });
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

  }
}
