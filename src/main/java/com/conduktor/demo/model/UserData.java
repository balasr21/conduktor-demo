package com.conduktor.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;
import java.util.List;

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
