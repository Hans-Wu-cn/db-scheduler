package com.github.kagkarlsson.scheduler.solon.integration;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.solon.JobManager;
import com.github.kagkarlsson.scheduler.solon.autoconfig.DbSchedulerAutoConfig;
import com.github.kagkarlsson.scheduler.solon.config.DbSchedulerProperties;
import org.noear.solon.Solon;
import org.noear.solon.core.AppContext;
import org.noear.solon.core.Plugin;
import org.noear.solon.scheduling.annotation.EnableScheduling;
import org.noear.solon.scheduling.annotation.Scheduled;
import org.noear.solon.scheduling.scheduled.manager.IJobManager;
import org.noear.solon.scheduling.scheduled.manager.JobExtractor;

public class XPluginImpl implements Plugin {
    @Override
    public void start(AppContext context) {
        if (Solon.app().source().getAnnotation(EnableScheduling.class) == null) {
            return;
        }
      context.beanScan(DbSchedulerAutoConfig.class);
      context.beanScan(DbSchedulerProperties.class);
        //注册 IJobManager
        context.wrapAndPut(IJobManager.class, JobManager.getInstance());

        //允许产生 Scheduler bean
        context.getBeanAsync(Scheduler.class, bean -> {
            JobManager.getInstance().setScheduler(bean);
        });

        //提取任务
        JobExtractor jobExtractor = new JobExtractor(JobManager.getInstance());
        context.beanBuilderAdd(Scheduled.class, ((clz, bw, anno) -> {
          System.out.println("1");
//            if (bw.raw() instanceof Job) {
//                Method method = Job.class.getDeclaredMethods()[0];
//                jobExtractor.doExtract(bw, method, anno);
//            } else {
//                jobExtractor.doBuild(clz, bw, anno);
//            }
        }));
        context.beanExtractorAdd(Scheduled.class, jobExtractor);

        //容器加载完后，再启动任务
        context.lifecycle(Integer.MAX_VALUE, () -> {
            JobManager.getInstance().start();
        });
    }

    @Override
    public void stop() throws Throwable {
        JobManager.getInstance().stop();
    }
}
