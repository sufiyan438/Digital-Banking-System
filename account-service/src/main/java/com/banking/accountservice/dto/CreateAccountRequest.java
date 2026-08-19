package com.banking.accountservice.dto;

import com.banking.accountservice.model.AccountStatus;
import com.banking.accountservice.model.AccountType;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateAccountRequest {
    @NotBlank(message = "Account holder name is required")
    private String accountHolderName;

    @Email(message = "Invalid email format")
    @NotBlank(message = "Email is required")
    private String email;

    @NotBlank(message = "Phone number is required for contact")
    private String phone;

    @NotBlank(message = "Account type should be specified")
    private AccountType accountType;

    @NotNull(message = "Initial deposit is required")
    @Positive(message = "Deposit must be positive")
    private BigDecimal initialDeposit;
}
