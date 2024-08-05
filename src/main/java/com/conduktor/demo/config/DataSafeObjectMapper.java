package com.conduktor.demo.config;

import com.conduktor.demo.exception.InvalidDataException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DataSafeObjectMapper {

  private final ObjectMapper objectMapper;

  public <T> T readValue(String content, Class<T> valueType) {
    try {
      return objectMapper.readValue(content, valueType);
    } catch (JsonProcessingException e) {
      throw new InvalidDataException(e.getMessage(), e);
    }
  }

  public String writeValueAsString(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new InvalidDataException(e.getMessage(), e);
    }
  }
}
