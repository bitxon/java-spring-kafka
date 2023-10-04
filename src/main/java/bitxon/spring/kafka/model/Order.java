package bitxon.spring.kafka.model;

public record Order(
    String product,
    Integer quantity
) {}
