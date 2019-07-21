package com.aliware.tianchi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientStatus {
    private static final ConcurrentMap<String, ClientStatus> SERVICE_STATISTICS = new ConcurrentHashMap<>();

    final AtomicInteger activeCount = new AtomicInteger(0);
    final AtomicInteger rtt = new AtomicInteger(0);

    private ClientStatus() {
    }

    public static void requestCount(String key) {
        ClientStatus clientStatus = getStatus(key);
        clientStatus.activeCount.incrementAndGet();
    }

    public static ClientStatus getStatus(String key) {
        ClientStatus status = SERVICE_STATISTICS.get(key);
        if (status == null) {
            SERVICE_STATISTICS.putIfAbsent(key, new ClientStatus());
            status = SERVICE_STATISTICS.get(key);
        }
        return status;
    }

    public static void responseCount(String key, int rtt1) {
        ClientStatus clientStatus = getStatus(key);
        clientStatus.activeCount.decrementAndGet();
        clientStatus.rtt.set(rtt1);
    }


    public static ConcurrentMap<String, ClientStatus> getServiceStatistics() {
        return SERVICE_STATISTICS;
    }

    @Override
    public String toString() {
        return "{" +
                ", activeCount=" + activeCount +
                ", rtt=" + rtt +
                '}';
    }
}