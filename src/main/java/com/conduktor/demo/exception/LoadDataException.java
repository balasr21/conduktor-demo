package com.conduktor.demo.exception;

public class LoadDataException extends RuntimeException {

  public LoadDataException(String message) {
    super(message);
  }

  public LoadDataException(String message, Throwable cause) {
    super(message, cause);
  }

}
