package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import static java.util.Map.Entry.comparingByValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
        Invoker<T> invoker = doSelect(invokers, url, invocation);
//        System.out.println(invoker.getUrl());
        return invoker;
    }
    public <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
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
        long maxLeft = Long.MIN_VALUE;
//        Invoker<T> selectedInvoker = null;

        int totalWeight = 0;
        int firstWeight = 0;
        int[] equalIndexs = new int[iSize];
        String[] wightKey = new String[iSize];
        Map<String,Integer> map = new HashMap<>();
        int equalCount = 0;
        boolean equalWeight = true;
        ConcurrentMap<String, ClientStatus> tempMap = ClientStatus.getServiceStatistics();
        for (int i=0;i<iSize;i++) {
            Invoker<T> invoker = invokers.get(i);
            URL url1 = invoker.getUrl();
            String ip = url1.getIp();
            int port = url1.getPort();
            String key = ip + port;
            ClientStatus clientStatus = tempMap.get(key);
            int initWeight = MAX_THREAD_MAP.get(key);
            int left = initWeight - clientStatus.activeCount.get();

            if (maxLeft <left) {
                maxLeft = left;
//                selectedInvoker = invoker;
                equalIndexs[0] = i;
                wightKey[0]=key;
                equalCount=1;
                totalWeight = initWeight;
                firstWeight = initWeight;
                equalWeight = true;
            }else if(maxLeft==left){
                map.put(key,left);
                equalIndexs[equalCount] = i;
                wightKey[equalCount] = key;
                equalCount++;
                totalWeight += initWeight;
                if (equalWeight && i > 0
                        && initWeight != firstWeight) {
                    equalWeight = false;
                }
            }
        }
        if (equalCount == 1) {
            return invokers.get(equalIndexs[0]);
        }
        Invoker<T> selectedInvoker = null;
//        System.out.println(map);
        if (!equalWeight && totalWeight > 0) {
            int maxWeight = 0;
            for (int i = 0; i < equalCount; i++) {
                int equalIndex = equalIndexs[i];
                if(maxWeight<MAX_THREAD_MAP.get(wightKey[i])){
                    maxWeight=MAX_THREAD_MAP.get(wightKey[i]);
                    selectedInvoker=invokers.get(equalIndex);
                }
            }
            if(selectedInvoker!=null){
                return selectedInvoker;
            }
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
            if (left>0 && maxLeft < rtt) {
                maxLeft = rtt;
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
