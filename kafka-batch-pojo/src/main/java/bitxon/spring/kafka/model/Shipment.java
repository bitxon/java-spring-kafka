package bitxon.spring.kafka.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record Shipment(
    @NotBlank
    String address,
    @NotNull
    @Positive
    Integer trackingNumber
) {}
