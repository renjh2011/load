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

//import org.apache.dubbo.common.logger.Logger;
//import org.apache.dubbo.common.logger.LoggerFactory;
//import org.apache.dubbo.rpc.RpcStatus;
//import java.util.ArrayList;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    protected static class WeightedRoundRobin {
        private int weight;
        private AtomicLong current = new AtomicLong(0);
        /**
         * 如果长时间
         */
        private int initWeight;
        public int getWeight() {
            return weight;
        }

        public WeightedRoundRobin(int weight, Long current) {
            this.weight = weight;
            this.current.set(current);
        }
        public WeightedRoundRobin(int initWeight,int weight, Long current) {
            this.initWeight = initWeight;
            this.weight = weight;
            this.current.set(current);
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }
        public long increaseCurrent() {
            return current.addAndGet(weight);
        }
        public void sel(int total) {
            current.addAndGet(-1 * total);
        }

        public void setCurrent(long current) {
            this.current.set(current);
        }

        public int getInitWeight() {
            return initWeight;
        }

        public void setInitWeight(int initWeight) {
            this.initWeight = initWeight;
        }

        @Override
        public String toString() {
            return "{" +
                    "weight=" + weight +
                    ", current=" + current +
                    '}';
        }
    }

    public static ConcurrentMap<String, UserLoadBalance.WeightedRoundRobin> methodWeightMap = new ConcurrentHashMap<String, UserLoadBalance.WeightedRoundRobin>();


    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int wSize = methodWeightMap.size();
        int iSize = invokers.size();
        //如果权重信息没有加载完 随机
        if(wSize!=iSize){
            Invoker<T> invoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            return invoker;
        }

        long maxCurrent = Long.MIN_VALUE;

        ConcurrentMap<String, ClientStatus> tempMap = new ConcurrentHashMap<>(ClientStatus.getServiceStatistics());

        Invoker<T> selectedInvoker = null;
        for (Invoker<T> invoker : invokers) {
            URL url1 = invoker.getUrl();
            String ip = url1.getIp();
            int port = url1.getPort();
            String key = ip + port;
            UserLoadBalance.WeightedRoundRobin weightedRoundRobin = methodWeightMap.get(key);
            ClientStatus clientStatus = tempMap.get(key);
            int initWeight = weightedRoundRobin.getInitWeight();
            int left = initWeight-clientStatus.activeCount.get();
            int tempRtt = clientStatus.rtt.get();
            if(left>initWeight*2/5){
                return invoker;
            }
            if(left<=2 || tempRtt>200){
                continue;
            }
            if(maxCurrent<left){
                maxCurrent=left;
                selectedInvoker=invoker;
            }
        }
        if(selectedInvoker!=null){
            return selectedInvoker;
        }
        maxCurrent = Long.MIN_VALUE;
        tempMap = new ConcurrentHashMap<>(ClientStatus.getServiceStatistics());
        for (Invoker<T> invoker : invokers) {
            URL url1 = invoker.getUrl();
            String ip = url1.getIp();
            int port = url1.getPort();
            String key = ip + port;
            UserLoadBalance.WeightedRoundRobin weightedRoundRobin = methodWeightMap.get(key);
            ClientStatus clientStatus = tempMap.get(key);
            int left = weightedRoundRobin.getWeight()-clientStatus.activeCount.get();
            int rtt = clientStatus.rtt.get();
            if (left>0 && maxCurrent < rtt) {
                maxCurrent = rtt;
                selectedInvoker = invoker;
            }
        }
        if(selectedInvoker!=null){
//             System.out.println(selectedInvoker.getUrl().getPort());
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(ThreadLocalRandom.current().nextInt(iSize));
    }

}
