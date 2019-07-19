package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerStatus {
    private static AtomicInteger MAX_THREAD_POOL = new AtomicInteger(0);
    private static ServerStatus status = new ServerStatus();


    private ServerStatus(){
        DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        Map<String, Object> map = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
        Set<Map.Entry<String, Object>> set = map.entrySet();
        for(Map.Entry<String, Object> entry1 : set){
            if(entry1.getValue() instanceof ThreadPoolExecutor){
                ThreadPoolExecutor executorService = (ThreadPoolExecutor) entry1.getValue();
                MAX_THREAD_POOL.set(executorService.getMaximumPoolSize());
                break;
            }
        }
    }

    public static Integer getMaxThreadPool() {
        return MAX_THREAD_POOL.get();
    }

}
