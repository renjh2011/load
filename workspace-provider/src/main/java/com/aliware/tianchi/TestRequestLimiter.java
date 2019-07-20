package com.aliware.tianchi;

import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;

public class TestRequestLimiter implements RequestLimiter {
    private static int MAX_LIMIT_RT = 500;
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {
        return true;
    }
}
