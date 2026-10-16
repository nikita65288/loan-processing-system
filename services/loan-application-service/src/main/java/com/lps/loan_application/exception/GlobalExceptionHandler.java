package com.lps.loan_application.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ApplicationNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ApiError handleNotFound(
            ApplicationNotFoundException exception,
            HttpServletRequest request
    ) {
        return new ApiError(
                Instant.now(),
                404,
                "APPLICATION_NOT_FOUND",
                exception.getMessage(),
                request.getRequestURI(),
                List.of()
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiError handleValidation(
            MethodArgumentNotValidException exception,
            HttpServletRequest request
    ) {

        List<ApiError.FieldError> fields =
                exception.getBindingResult()
                        .getFieldErrors()
                        .stream()
                        .map(error ->
                                new ApiError.FieldError(
                                        error.getField(),
                                        error.getDefaultMessage()
                                )
                        )
                        .toList();

        return new ApiError(
                Instant.now(),
                400,
                "VALIDATION_ERROR",
                "Request validation failed",
                request.getRequestURI(),
                fields
        );
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ApiError handleUnexpected(
            Exception exception,
            HttpServletRequest request
    ) {
        return new ApiError(
                Instant.now(),
                500,
                "INTERNAL_SERVER_ERROR",
                "An unexpected error occurred",
                request.getRequestURI(),
                List.of()
        );
    }

}
