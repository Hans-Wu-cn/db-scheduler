package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Instant;
import org.noear.solon.Solon;
import org.noear.solon.annotation.Bean;
import org.noear.solon.annotation.SolonMain;
import org.noear.solon.scheduling.annotation.EnableScheduling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolonMain
@EnableScheduling
public class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    Solon.start(App.class, args);
  }
  @Bean
  void executeOnStartup(Scheduler scheduler, Task<Void> sampleOneTimeTask) {
    log.info("Scheduling one time task to now!");

    scheduler.schedule(sampleOneTimeTask.instance("command-line-runner"), Instant.now());
  }
}
