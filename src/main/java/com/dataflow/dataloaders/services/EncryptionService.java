package com.dataflow.dataloaders.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

@Slf4j
@Service
public class EncryptionService {

    private static final String ALGORITHM = "AES";
    private static final String KEY = "DataManager2024!"; // Use environment variable in production

    @Autowired
    private ObjectMapper objectMapper;

    public JsonNode encrypt(JsonNode data) {
        try {
            String jsonString = objectMapper.writeValueAsString(data);
            SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encrypted = cipher.doFinal(jsonString.getBytes());
            String encryptedString = Base64.getEncoder().encodeToString(encrypted);
            return objectMapper.readTree("\"" + encryptedString + "\"");
        } catch (Exception e) {
            log.error("Encryption failed: {}", e.getMessage());
            return data;
        }
    }

    public JsonNode decrypt(JsonNode encryptedData) {
        try {
            if (encryptedData == null || !encryptedData.isTextual()) {
                return encryptedData;
            }
            String decryptedString = decryptString(encryptedData.asText());
            return objectMapper.readTree(decryptedString);
        } catch (Exception e) {
            log.error("Decryption failed: {}", e.getMessage());
            return encryptedData;
        }
    }

    public String encryptString(String data) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encrypted = cipher.doFinal(data.getBytes());
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            log.error("String encryption failed: {}", e.getMessage());
            return data;
        }
    }

    public String decryptString(String encryptedData) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(encryptedData));
            return new String(decrypted);
        } catch (Exception e) {
            log.error("String decryption failed: {}", e.getMessage());
            return encryptedData;
        }
    }
}
