package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
/**
 * @author daofeng.xjf
 *
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
        long start = System.currentTimeMillis();
        ClientStatus.requestCount(ip+port);
        try{
            Result result = invoker.invoke(invocation);
            return result;
        }catch (Exception e){
            ClientStatus.responseCount(ip+port,true,(int)(System.currentTimeMillis()-start));
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        String ip = invoker.getUrl().getIp();
        int port = invoker.getUrl().getPort();
        boolean isFailed = false;
        //初始化每个provider对应的线程池
        if(!result.hasException() && UserLoadBalance.methodWeightMap.get(ip+port)==null){
            String maxThreadPool = result.getAttachment(ip+port+"maxPool");
            UserLoadBalance.methodWeightMap.put(ip+port,new UserLoadBalance.WeightedRoundRobin(Integer.parseInt(maxThreadPool),Integer.parseInt(maxThreadPool),0L));
        }
        String rtt = result.getAttachment(ip+port+"rtt");
        if(result.hasException()){
            isFailed=true;
            rtt="1000";
        }

        ClientStatus.responseCount(ip+port, isFailed,rtt==null || rtt.isEmpty() ? 0 :  Integer.parseInt(rtt));
        return result;
    }
}