package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        String ip = invoker.getUrl().getIp();
        int port = invoker.getUrl().getPort();
        String key = ip + port;
        long start = System.currentTimeMillis();
        ClientStatus.requestCount(key);
        try {
            Result result = invoker.invoke(invocation);
            return result;
        } catch (Exception e) {
            ClientStatus.responseCount(key, true, (int) (System.currentTimeMillis() - start));
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String ip = invoker.getUrl().getIp();
        int port = invoker.getUrl().getPort();
        String key = ip + port;
        boolean isFailed = false;
        //初始化每个provider对应的线程池
        if (!result.hasException() && UserLoadBalance.MAX_THREAD_MAP.get(key) == null) {
            String maxThreadPool = result.getAttachment(key + "maxPool");
            UserLoadBalance.MAX_THREAD_MAP.put(key, maxThreadPool == null ? 100 : Integer.parseInt(maxThreadPool));
        }
        String rtt = result.getAttachment(key + "rtt");
        if (result.hasException()) {
            isFailed = true;
            rtt = "1000";
        }

        ClientStatus.responseCount(ip + port, isFailed, rtt == null || rtt.isEmpty() ? 0 : Integer.parseInt(rtt));
        return result;
    }
}