package com.github.kagkarlsson.scheduler.solon;

import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.fixedDelay;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import com.github.kagkarlsson.scheduler.exceptions.DbSchedulerException;
import com.github.kagkarlsson.scheduler.solon.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import org.noear.snack.ONode;
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
  public static final TaskWithDataDescriptor<ONode> STATE_TRACKING_RECURRING_TASK =
    new TaskWithDataDescriptor<>("state-tracking-recurring-task", ONode.class);

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

  private Optional<ScheduledExecution<Object>> getScheduledExecution(
    TaskInstanceId taskInstanceId) {
    return scheduler.getScheduledExecution(taskInstanceId);
  }


  public Optional<TaskInstanceId> exists(String name) throws DbSchedulerException {
    if (scheduler != null) {
      TaskInstanceId taskInstanceId = buildTaskInstanceId(name);
      return getScheduledExecution(taskInstanceId).isPresent() ? Optional.of(taskInstanceId)
        : Optional.empty();
    } else {
      return Optional.empty();
    }
  }

  public void resume(TaskInstanceId taskInstanceId) throws DbSchedulerException {
    if (scheduler != null) {
      scheduler.reschedule(taskInstanceId,
        Instant.now());
    }
  }

  /**
   * 注册 job（on start）
   */
  public void register(JobHolder jobHolder) throws DbSchedulerException {
    if (Utils.isEmpty(jobHolder.getScheduled().cron())) {
      regJobByFixedRate(jobHolder, jobHolder.getScheduled().fixedRate());
    } else {
      regJobByCron(jobHolder, jobHolder.getScheduled().cron(), jobHolder.getScheduled().zone());
    }
  }

  public void pause(String name) throws DbSchedulerException {
      throw new RuntimeException("DbSchedule currently does not support pausing tasks");
  }

  /**
   * 移除 job
   */
  public void remove(String name) throws DbSchedulerException {
    if (scheduler != null) {
      scheduler.cancel(buildTaskInstanceId(name));
    }
  }

  private void regJobByCron(JobHolder jobHolder, String cron, String zone)
    throws DbSchedulerException {
//    tryInitScheduler();

    if (exists(jobHolder.getScheduled().name()).isEmpty()) {
      ZoneId zoneId = ZoneId.systemDefault();
      //支持时区配置
      if (Utils.isNotEmpty(zone)) {
        zoneId = ZoneId.of(zone);
      }
      RecurringTask<ONode> execute = Tasks.recurring(STATE_TRACKING_RECURRING_TASK,
          Schedules.cron(cron, zoneId))
        .execute((taskInstance, executionContext) -> {
          try {
            jobHolder.handle(DbSchedulerContext.getContext(taskInstance));
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        });
    }
  }

  private void regJobByFixedRate(JobHolder jobHolder, long milliseconds)
    throws DbSchedulerException {
//    tryInitScheduler();
    if (exists(jobHolder.getScheduled().name()).isEmpty()) {
      Tasks.recurring(STATE_TRACKING_RECURRING_TASK,
          fixedDelay(Duration.ofMinutes(milliseconds)))
        .execute((taskInstance, executionContext) -> {
          try {
            jobHolder.handle(DbSchedulerContext.getContext(taskInstance));
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        });
    }
  }

  private TaskInstanceId buildTaskInstanceId(String name){
    return TaskInstanceId.of(RecurringTask.INSTANCE, name);
  }

//  private void tryInitScheduler() throws DbSchedulerException {
//    if (scheduler == null) {
//      synchronized (this) {
//        if (scheduler == null) {
//          //默认使用：直接本地调用
//          SchedulerFactory schedulerFactory = new StdSchedulerFactory();
//          scheduler = schedulerFactory.getScheduler();
//        }
//      }
//    }
//  }
}
