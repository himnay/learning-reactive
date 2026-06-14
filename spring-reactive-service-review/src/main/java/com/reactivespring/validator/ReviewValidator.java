package com.reactivespring.validator;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.exception.ReviewDataException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;

import static java.util.stream.Collectors.joining;

@Component
public class ReviewValidator {

    private static final Logger log = LoggerFactory.getLogger(ReviewValidator.class);

    private final Validator validator;

    public ReviewValidator(Validator validator) {
        this.validator = validator;
    }

    public void validate(ReviewDocument reviewDocument) {
        Set<ConstraintViolation<ReviewDocument>> violations = validator.validate(reviewDocument);
        if (!violations.isEmpty()) {
            log.error("Constraint violations: {}", violations);
            var errorMessage = violations.stream()
                    .map(ConstraintViolation::getMessage)
                    .sorted()
                    .collect(joining(","));
            throw new ReviewDataException(errorMessage);
        }
    }
}
