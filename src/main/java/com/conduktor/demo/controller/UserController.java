package com.conduktor.demo.controller;

import com.conduktor.demo.model.UserData;
import com.conduktor.demo.service.UserService;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
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
    return new ResponseEntity<>(userService.peek(topicName, offset, limit), HttpStatus.OK);
  }
}
