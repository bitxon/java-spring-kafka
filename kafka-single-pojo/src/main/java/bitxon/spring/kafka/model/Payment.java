package bitxon.spring.kafka.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record Payment(
    @NotNull
    String message,
    @NotNull
    @PositiveOrZero
    Integer amount
) {}
