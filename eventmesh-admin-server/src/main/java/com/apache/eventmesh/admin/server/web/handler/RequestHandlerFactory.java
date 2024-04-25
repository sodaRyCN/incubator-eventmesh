package com.apache.eventmesh.admin.server.web.handler;

import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import org.apache.eventmesh.common.adminserver.request.BaseRequest;
import org.apache.eventmesh.common.adminserver.response.BaseResponse;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RequestHandlerFactory implements ApplicationListener<ContextRefreshedEvent> {

    private final Map<String, AbstractRequestHandler<BaseRequest,BaseResponse>> handlers =
            new ConcurrentHashMap<>();

    public AbstractRequestHandler<BaseRequest, BaseResponse> getHandler(String type) {
        return handlers.get(type);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Map<String, AbstractRequestHandler> beans =
                event.getApplicationContext().getBeansOfType(AbstractRequestHandler.class);

        for (AbstractRequestHandler<BaseRequest, BaseResponse> requestHandler : beans.values()) {
            Class<?> clazz = requestHandler.getClass();
            boolean skip = false;
            while (!clazz.getSuperclass().equals(AbstractRequestHandler.class)) {
                if (clazz.getSuperclass().equals(Object.class)) {
                    skip = true;
                    break;
                }
                clazz = clazz.getSuperclass();
            }
            if (skip) {
                continue;
            }

            try {
                Method method = clazz.getMethod("handle", Request.class, RequestMeta.class);
            } catch (Exception e) {
                //ignore.
            }
            Class tClass = (Class) ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments()[0];
            handlers.putIfAbsent(tClass.getSimpleName(), requestHandler);
        }
    }
}
