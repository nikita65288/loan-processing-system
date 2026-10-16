package com.lps.loan_application.dto;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateLoanApplicationRequest(

        @NotNull
        UUID customerId,

        @NotNull
        @DecimalMin(value = "0.01")
        @Digits(integer = 17, fraction = 2)
        BigDecimal amount,

        @NotNull
        @Min(1)
        @Max(120)
        Integer term,

        @NotBlank
        @Size(min = 3, max = 3)
        @Pattern(regexp = "[A-Z]{3}")
        String currency
) {}
