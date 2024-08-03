package com.conduktor.demo.model;

import java.time.LocalDate;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record UserData(
    @JsonProperty("_id") String id,
    String name,
    @JsonProperty("dob") LocalDate dateOfBirth,
    Address address,
    String telephone,
    List<String> pets,
    float score,
    String email,
    String url,
    String description,
    boolean verified,
    int salary) {}
