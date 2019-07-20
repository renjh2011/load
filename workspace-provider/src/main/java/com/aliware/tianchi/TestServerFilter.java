package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author daofeng.xjf
 *
 * 服务端过滤器
 * 可选接口
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {
    volatile static long RTT = 0;
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try{
            invocation.getAttachments().put("start",System.currentTimeMillis()+"");
            Result result = invoker.invoke(invocation);
            return result;
        }catch (Exception e){
            throw e;
        }

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        int port = invoker.getUrl().getPort();
        String ip = invoker.getUrl().getIp();
        int rtt =(int)(System.currentTimeMillis()-Long.valueOf(invocation.getAttachments().get("start")));
        Map<String,String> map = new HashMap<>(2);
        map.put(ip+port+"rtt",rtt+"");
        map.put(ip+port+"maxPool",(ServerStatus.getMaxThreadPool()+""));
        result.addAttachments(map);
        RTT=rtt;
        return result;
    }

}
