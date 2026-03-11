package org.example.model;

/**
 * Customer identity details used for API display.
 */
public record CustomerName(
        String customerId,
        String firstName,
        String middleName,
        String lastName
) {
    public String displayName() {
        return firstName + " " + middleName + " " + lastName;
    }
}
