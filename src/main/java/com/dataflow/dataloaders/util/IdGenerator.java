package com.dataflow.dataloaders.util;

import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.Base64;

@Component
public class IdGenerator {

    private static final SecureRandom random = new SecureRandom();

    public String generateId() {
        byte[] bytes = new byte[16]; // 16 bytes = 128 bits
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); // e.g., xNjDZFMXoAIaAXYRBXLrt
    }
}
