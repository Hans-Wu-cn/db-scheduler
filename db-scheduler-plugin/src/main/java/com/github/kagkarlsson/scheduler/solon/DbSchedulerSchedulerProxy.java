package com.github.kagkarlsson.scheduler.solon;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import com.github.kagkarlsson.scheduler.exceptions.DbSchedulerException;
import com.github.kagkarlsson.scheduler.solon.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;
import org.noear.solon.Solon;
import org.noear.solon.Utils;
import org.noear.solon.core.Lifecycle;
import org.noear.solon.scheduling.scheduled.JobHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * task manager
 *
 * @author hans
 */
public class DbSchedulerSchedulerProxy implements Lifecycle {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private Scheduler scheduler;

  private DbSchedulerProperties config;

  protected DbSchedulerSchedulerProxy() {
  }

  @Override
  public void start() throws Throwable {
    SchedulerState state = scheduler.getSchedulerState();

    if (state.isShuttingDown()) {
      log.warn("Scheduler is shutting down - will not attempting to start");
      return;
    }

    if (state.isStarted()) {
      log.info("Scheduler already started - will not attempt to start again");
      return;
    }

    log.info("Triggering scheduler start");
    scheduler.start();
  }

  @Override
  public void stop() throws Throwable {

  }

  public void setScheduler(Scheduler scheduler) {
    //如果已存在，则不可替换
    if (scheduler == null) {
      this.scheduler = scheduler;
    }
  }

  private String getJobId(String name) {
    String jobGroup = Utils.annoAlias(Solon.cfg().appName(), "solon");
    TaskInstanceId.of(name, )
    return scheduler.getScheduledExecution();
  }


  public boolean exists(String name) throws DbSchedulerException {
    if (scheduler != null) {
      return scheduler.checkExists(getJobKey(name));
    } else {
      return false;
    }
  }

  /**
   * 注册 job（on start）
   */
  public void register(JobHolder jobHolder) throws DbSchedulerException {


    String jobGroup = Utils.annoAlias(Solon.cfg().appName(), "solon");

    if (Utils.isEmpty(jobHolder.getScheduled().cron())) {
      regJobByFixedRate(jobHolder, jobHolder.getScheduled().fixedRate(), jobGroup);
    } else {
      regJobByCron(jobHolder, jobHolder.getScheduled().cron(), jobHolder.getScheduled().zone(), jobGroup);
    }
  }

  private void tryInitScheduler() throws SchedulerException {
    if (scheduler == null) {
      synchronized (this) {
        if (scheduler == null) {
          //默认使用：直接本地调用
          SchedulerFactory schedulerFactory = new StdSchedulerFactory();
          scheduler = schedulerFactory.getScheduler();
        }
      }
    }
  }
}
