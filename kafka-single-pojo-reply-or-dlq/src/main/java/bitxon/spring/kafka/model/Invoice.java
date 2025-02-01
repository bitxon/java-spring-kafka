package bitxon.spring.kafka.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record Invoice(
    @NotNull
    @PositiveOrZero
    Integer id,
    @NotNull
    String message
) {}
