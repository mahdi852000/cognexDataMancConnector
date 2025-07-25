package org.example.akka.utils;

import net.enilink.komma.core.IReference;
import org.example.akka.extra.IResource;
import org.example.akka.extra.LOGISTICS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ScannerUtils {
    private static final Logger logger = LoggerFactory.getLogger(ScannerUtils.class);

    public static <T> Optional<T> getProperty(IResource delegate, Class<T> clazz, String propertyName) {
        IReference ref = LOGISTICS.NAMAESPACE_URI.appendLocalPart(propertyName);
        Object value = delegate.getSingle(ref);
        if (value == null) return Optional.empty();

        try {
            if (clazz == Long.class) {
                return Optional.of(clazz.cast(Long.valueOf(value.toString())));
            } else if (clazz == Boolean.class) {
                return Optional.of(clazz.cast(Boolean.valueOf(value.toString())));
            }
        } catch (Exception e) {
            logger.warn("Failed to convert property {} to type {}", propertyName, clazz.getSimpleName(), e);
        }

        return Optional.empty();
    }
}
