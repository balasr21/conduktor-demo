package com.conduktor.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Address(String street, String town, @JsonProperty("postode") String postcode) {}
