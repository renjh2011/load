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

//    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
//        Invoker<T> invoker = doSelect(invokers, url, invocation);
////        System.out.println(invoker.getUrl());
//        return invoker;
//    }
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int wSize = MAX_THREAD_MAP.size();
        int iSize = invokers.size();
        //如果权重信息没有加载完 随机
        if(wSize!=iSize){
            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }

        int equalCount=0;
        int[] equalIndexs=new int[iSize];
        int[] weights=new int[iSize];
        boolean equalLeft = true;
        int firstLeft = 0;

        long maxCurrent = Long.MIN_VALUE;
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
            int tempRtt = clientStatus.rtt.get();
            //如果线程数够多 直接返回
            /*if (left > initWeight * 2 / 5) {
                return invoker;
            }
            //如果剩余可用线程太少 或者 响应时间太大，不优先使用该线程
            if (left <= 2 || tempRtt > 150) {
                continue;
            }*/
            if (maxCurrent <left) {
                equalCount=1;
                equalIndexs[0]=i;
                weights[0]=tempRtt;
                maxCurrent = left;
                firstLeft=initWeight;
                equalLeft=true;
//                selectedInvoker = invoker;
            }else if(maxCurrent==left){
                equalIndexs[equalCount] = i;
                weights[equalCount] = initWeight;
                equalCount++;
                if (equalLeft  && initWeight != firstLeft) {
                    equalLeft = false;
                }
            }
        }
        if (equalCount == 1) {
            return invokers.get(equalIndexs[0]);
        }

        //换成weight或者rtt
        int minWeight = Integer.MAX_VALUE;
        Invoker<T> selectedInvoker = null;
        for (int i = 0; i < equalCount; i++) {
            int equalIndex = equalIndexs[i];
            int weight = weights[i];
            if(minWeight>weight){
                minWeight=weight;
                selectedInvoker=invokers.get(equalIndex);
            }
        }
        if(selectedInvoker!=null){
//            System.out.println(selectedInvoker.getUrl());
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
