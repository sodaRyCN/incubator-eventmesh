package org.apache.eventmesh.registry;

import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.spi.EventMeshExtensionFactory;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RegistryFactory {
    private static final Map<String, RegistryService> META_CACHE = new HashMap<>(16);

    public static RegistryService getInstance(String registryPluginType) {
        return META_CACHE.computeIfAbsent(registryPluginType, RegistryFactory::registryBuilder);
    }

    private static RegistryService registryBuilder(String registryPluginType) {
        RegistryService registryServiceExt = EventMeshExtensionFactory.getExtension(RegistryService.class, registryPluginType);
        if (registryServiceExt == null) {
            String errorMsg = "can't load the metaService plugin, please check.";
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        return registryServiceExt;
    }
}
