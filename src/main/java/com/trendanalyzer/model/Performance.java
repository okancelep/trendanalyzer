package com.trendanalyzer.model;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class Performance {
    private String key;
    private int count;
    private BigDecimal sum;
}
