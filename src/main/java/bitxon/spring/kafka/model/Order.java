package bitxon.spring.kafka.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record Order(
    @NotBlank
    String product,
    @NotNull
    @Positive
    Integer quantity
) {}
