package com.reactivespring.validator;

import com.reactivespring.entity.ReviewDocument;
import com.reactivespring.exception.ReviewDataException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.util.Set;

import static java.util.stream.Collectors.joining;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReviewValidator {

    private final Validator validator;

    public void validator(ReviewDocument reviewDocument) {
        Set<ConstraintViolation<ReviewDocument>> constraintViolations = validator.validate(reviewDocument);
        if (constraintViolations.size() > 0) {
            log.error("Constraint Violation [ {} ]", constraintViolations);
            var errorMessage = constraintViolations.stream()
                    .map(ConstraintViolation::getMessage)
                    .sorted()
                    .collect(joining(","));
            throw new ReviewDataException(errorMessage);
        }
    }
}
