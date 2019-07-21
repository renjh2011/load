package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */

public class UserLoadBalance implements LoadBalance {

    /**
     * ConcurrentMap<ip+port,max_pool_size>
     */
    static ConcurrentMap<String, Integer> MAX_THREAD_MAP = new ConcurrentHashMap<>();


    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int wSize = MAX_THREAD_MAP.size();
        int iSize = invokers.size();
        //如果权重信息没有加载完 随机
        if(wSize!=iSize){
            Invoker<T> invoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            return invoker;
        }

        /*
         * 多寻找几次可用性高的provider
         */
        long maxCurrent = Long.MIN_VALUE;
        Invoker<T> selectedInvoker = null;
        ConcurrentMap<String, ClientStatus> tempMap = ClientStatus.getServiceStatistics();
        for (Invoker<T> invoker : invokers) {
            URL url1 = invoker.getUrl();
            String ip = url1.getIp();
            int port = url1.getPort();
            String key = ip + port;
            ClientStatus clientStatus = tempMap.get(key);
            int initWeight = MAX_THREAD_MAP.get(key);
            int left = initWeight - clientStatus.activeCount.get();
            int tempRtt = clientStatus.rtt.get();
            //如果线程数够多 直接返回
            if (left > initWeight * 2 / 5) {
                return invoker;
            }
            //如果剩余可用线程太少 或者 响应时间太大，不优先使用该线程
            if (left <= 10 || tempRtt > 150) {
                continue;
            }
            if (maxCurrent < left) {
                maxCurrent = left;
                selectedInvoker = invoker;
            }
        }
        if (selectedInvoker != null) {
            return selectedInvoker;
        }

        /*
         * 寻找rtt最小的provider
         */
        for (Invoker<T> invoker : invokers) {
            URL url1 = invoker.getUrl();
            String ip = url1.getIp();
            int port = url1.getPort();
            String key = ip + port;
            ClientStatus clientStatus = tempMap.get(key);
            int left =MAX_THREAD_MAP.get(key)-clientStatus.activeCount.get();
            int rtt = clientStatus.rtt.get();
            if (left>0 && maxCurrent < rtt) {
                maxCurrent = rtt;
                selectedInvoker = invoker;
            }
        }
        if(selectedInvoker!=null){
            return selectedInvoker;
        }

        //如果前面都没有
        return invokers.get(ThreadLocalRandom.current().nextInt(iSize));
    }

}
