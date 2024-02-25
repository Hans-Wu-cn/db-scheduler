package com.github.kagkarlsson.scheduler.solon;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.util.Map;
import java.util.Optional;
import org.noear.solon.Utils;
import org.noear.solon.scheduling.ScheduledException;
import org.noear.solon.scheduling.annotation.Scheduled;
import org.noear.solon.scheduling.scheduled.JobHolder;
import org.noear.solon.scheduling.scheduled.manager.AbstractJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * task manager
 *
 * @author hans
 */
public class JobManager extends AbstractJobManager {
  private static JobManager instance=new JobManager();

  /**
   * 获取实例
   */
  public static JobManager getInstance() {
    return instance;
  }


  private DbSchedulerSchedulerProxy schedulerProxy;

  public JobManager() {
    schedulerProxy = new DbSchedulerSchedulerProxy();
  }

  public void setScheduler(Scheduler real) {
    schedulerProxy.setScheduler(real);
  }

  @Override
  public void jobStart(String name, Map<String, String> data) throws ScheduledException {
    JobHolder holder = jobGet(name);

    if (holder != null) {
      holder.setData(data);

      try {
        Optional<TaskInstanceId> taskInstanceId = schedulerProxy.exists(name);
        if (taskInstanceId.isPresent()) {
          schedulerProxy.resume(taskInstanceId.get());
        } else {
          schedulerProxy.register(holder);
        }
      } catch (Exception e) {
        throw new ScheduledException(e);
      }
    }
  }

  @Override
  public void jobStop(String name) throws ScheduledException {
    if (jobExists(name)) {
      try {
        schedulerProxy.pause(name);
      } catch (Exception e) {
        throw new ScheduledException(e);
      }
    }
  }

  @Override
  public void jobRemove(String name) throws ScheduledException {
    if (jobExists(name)) {
      super.jobRemove(name);
      try {
        schedulerProxy.remove(name);
      } catch (Exception e) {
        throw new ScheduledException(e);
      }
    }
  }

  @Override
  protected void jobAddCheckDo(String name, Scheduled scheduled) {
    if (Utils.isEmpty(name)) {
      throw new IllegalArgumentException("The job name cannot be empty!");
    }

    if (scheduled.fixedRate() > 0 && Utils.isNotEmpty(scheduled.cron())) {
      throw new IllegalArgumentException("The job cron and fixedRate cannot both have values: " + name);
    }

    if (scheduled.initialDelay() > 0) {
      throw new IllegalArgumentException("The job unsupported initialDelay!");
    }

    if (scheduled.fixedDelay() > 0) {
      throw new IllegalArgumentException("The job unsupported fixedDelay!");
    }
  }

  @Override
  public void start() throws Throwable {
    for (JobHolder holder : jobMap.values()) {
      if (holder.getScheduled().enable()) {
        //只启动启用的（如果有需要，手动启用）
        schedulerProxy.register(holder);
      }
    }
    schedulerProxy.start();

    isStarted = true;
  }

  @Override
  public void stop() throws Throwable {
    isStarted = false;

    if (schedulerProxy != null) {
      schedulerProxy.stop();
      schedulerProxy = null;
    }
  }
}
