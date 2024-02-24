package com.github.kagkarlsson.scheduler.solon;

import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.util.Map;
import org.noear.snack.ONode;
import org.noear.solon.core.handle.Context;
import org.noear.solon.core.handle.ContextEmpty;
import org.noear.solon.core.handle.ContextUtil;

/**
 * @author noear
 * @since 2.2
 */
public class DbSchedulerContext {

  /**
   * 获取执行上下文
   */
  public static Context getContext(TaskInstance<ONode> taskInstance) {
    Context ctx = Context.current(); //可能是从上层代理已生成, v1.11
    if (ctx == null) {
      ctx = new ContextEmpty();
      ContextUtil.currentSet(ctx);
    }

    // 设置请求对象（mvc 时，可以被注入）
    if (ctx instanceof ContextEmpty) {
      ((ContextEmpty) ctx).request(taskInstance);
    }
    Map<String, ONode> data = taskInstance.getData().obj();
    for (String key : data.keySet()) {
      ctx.paramMap().put(key, data.get(key).getString());
    }

    return ctx;
  }
}
