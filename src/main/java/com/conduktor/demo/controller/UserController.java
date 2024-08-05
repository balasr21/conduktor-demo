package com.conduktor.demo.controller;

import com.conduktor.demo.exception.InvalidDataException;
import com.conduktor.demo.model.UserData;
import com.conduktor.demo.service.UserService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class UserController {

  private final UserService userService;

  public UserController(UserService userService) {
    this.userService = userService;
  }

  @GetMapping(value = "/topic/{topicName}/{offset}")
  public ResponseEntity<List<UserData>> peek(
      @PathVariable("topicName") String topicName,
      @PathVariable("offset") long offset,
      @RequestParam("count") int limit) {
    if (offset < 0 || limit < 0) {
      log.warn(
          "Invalid request received for topic {} offset {} limit {}", topicName, offset, limit);
      throw new InvalidDataException(
          "Invalid request received for topic " + topicName + " with invalid offset/limit");
    }
    return new ResponseEntity<>(userService.peek(topicName, offset, limit), HttpStatus.OK);
  }
}
