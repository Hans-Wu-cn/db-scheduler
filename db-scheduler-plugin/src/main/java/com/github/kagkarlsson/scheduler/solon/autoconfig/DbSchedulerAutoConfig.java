package com.github.kagkarlsson.scheduler.solon.autoconfig;

import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.solon.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.solon.config.DbSchedulerProperties;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.noear.snack.ONode;
import org.noear.solon.annotation.Bean;
import org.noear.solon.annotation.Condition;
import org.noear.solon.annotation.Configuration;
import org.noear.solon.annotation.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class DbSchedulerAutoConfig {

  private static final Logger log = LoggerFactory.getLogger(DbSchedulerAutoConfig.class);
  private static final Predicate<Task<?>> shouldBeStarted = task -> task instanceof OnStartup;
  private DbSchedulerProperties config;
  private DataSource dataSource;
  private List<Task<?>> configuredTasks;
  private static final Serializer SOLON_JAVA_SERIALIZER =
    new Serializer() {

      public byte[] serialize(Object data) {
        if (data == null) {
          return null;
        }
       return ONode.serialize(data).getBytes();
      }

      public <T> T deserialize(Class<T> clazz, byte[] serializedData) {
        if (serializedData == null) {
          return null;
        }
        String serializedStr = new String(serializedData);
        return ONode.deserialize(serializedStr);
      }
    };
  @Condition(onMissingBean = DbSchedulerCustomizer.class)
  @Bean
  public DbSchedulerCustomizer noopCustomizer() {
    return new DbSchedulerCustomizer() {};
  }
  @Bean
  @Condition(onMissingBean = Scheduler.class)
  public Scheduler scheduler(DbSchedulerCustomizer customizer,
    StatsRegistry registry,
    @Inject DbSchedulerProperties dbSchedulerProperties,
    @Inject DataSource dataSource,
    @Inject List<Task<?>> configuredTasks
  ) {
    log.info("Creating db-scheduler using tasks from Spring context: {}", configuredTasks);
    this.dataSource = dataSource;
    this.config = dbSchedulerProperties;
    this.configuredTasks = configuredTasks;
    // Ensure that we are using a transactional aware data source

    // Instantiate a new builder
    final SchedulerBuilder builder =
      Scheduler.create(dataSource, nonStartupTasks(configuredTasks));

    builder.threads(config.getThreads());

    // Polling
    builder.pollingInterval(config.getPollingInterval());

    // Polling strategy
    if (config.getPollingStrategy() == PollingStrategyConfig.Type.FETCH) {
      builder.pollUsingFetchAndLockOnExecute(
        config.getPollingStrategyLowerLimitFractionOfThreads(),
        config.getPollingStrategyUpperLimitFractionOfThreads());
    } else if (config.getPollingStrategy() == PollingStrategyConfig.Type.LOCK_AND_FETCH) {
      builder.pollUsingLockAndFetch(
        config.getPollingStrategyLowerLimitFractionOfThreads(),
        config.getPollingStrategyUpperLimitFractionOfThreads());
    } else {
      throw new IllegalArgumentException(
        "Unknown polling-strategy: " + config.getPollingStrategy());
    }

    builder.heartbeatInterval(config.getHeartbeatInterval());

    // Use scheduler name implementation from customizer if available, otherwise use
    // configured scheduler name (String). If both is absent, use the library default
    if (customizer.schedulerName().isPresent()) {
      builder.schedulerName(customizer.schedulerName().get());
    } else if (config.getSchedulerName() != null) {
      builder.schedulerName(new SchedulerName.Fixed(config.getSchedulerName()));
    }

    builder.tableName(config.getTableName());

    // Use custom serializer if provided. Otherwise use devtools friendly serializer.
    builder.serializer(customizer.serializer().orElse(SOLON_JAVA_SERIALIZER));

    // Use custom JdbcCustomizer if provided.
    builder.jdbcCustomization(
      customizer
        .jdbcCustomization()
        .orElse(new AutodetectJdbcCustomization(dataSource)));

    if (config.isImmediateExecutionEnabled()) {
      builder.enableImmediateExecution();
    }

    // Use custom executor service if provided
    customizer.executorService().ifPresent(builder::executorService);

    // Use custom due executor if provided
    customizer.dueExecutor().ifPresent(builder::dueExecutor);

    // Use housekeeper executor service if provided
    customizer.housekeeperExecutor().ifPresent(builder::housekeeperExecutor);

    builder.deleteUnresolvedAfter(config.getDeleteUnresolvedAfter());

    // Add recurring jobs and jobs that implements OnStartup
    builder.startTasks(startupTasks(configuredTasks));

    // Expose metrics
    builder.statsRegistry(registry);

    // Failure logging
    builder.failureLogging(config.getFailureLoggerLevel(), config.isFailureLoggerLogStackTrace());

    // Shutdown max wait
    builder.shutdownMaxWait(config.getShutdownMaxWait());

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T extends Task<?> & OnStartup> List<T> startupTasks(List<Task<?>> tasks) {
    return tasks.stream()
      .filter(shouldBeStarted)
      .map(task -> (T) task)
      .collect(Collectors.toList());
  }

  private static List<Task<?>> nonStartupTasks(List<Task<?>> tasks) {
    return tasks.stream().filter(shouldBeStarted.negate()).collect(Collectors.toList());
  }
}
