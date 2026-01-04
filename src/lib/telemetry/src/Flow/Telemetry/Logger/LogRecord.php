<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use Flow\Telemetry\Attributes;

/**
 * Value object representing a log record.
 *
 * LogRecord is an immutable data container for log information including
 * severity, body, attributes, timestamp, and exception details.
 *
 * All setter methods return a new instance for fluent configuration.
 *
 * Example usage:
 * ```php
 * $record = (new LogRecord())
 *     ->setSeverity(Severity::ERROR)
 *     ->setBody('Database connection failed')
 *     ->setAttribute('db.host', 'localhost')
 *     ->setAttribute('db.port', 5432)
 *     ->setException($exception);
 *
 * $logger->emit($record);
 * ```
 */
final readonly class LogRecord
{
    /**
     * @param Severity $severity The severity level
     * @param string $body The log message body
     * @param Attributes $attributes Log record attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     */
    public function __construct(
        public Severity $severity = Severity::INFO,
        public string $body = '',
        public Attributes $attributes = new Attributes(),
        public ?\DateTimeImmutable $timestamp = null,
        public ?\DateTimeImmutable $observedTimestamp = null,
    ) {
    }

    /**
     * Create a LogRecord from a normalized array representation.
     *
     * @param array{
     *     severity: int,
     *     body: string,
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     timestamp: null|string,
     *     observedTimestamp: null|string
     * } $data Normalized LogRecord data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            Severity::from($data['severity']),
            $data['body'],
            Attributes::fromArray($data['attributes']),
            $data['timestamp'] !== null ? new \DateTimeImmutable($data['timestamp']) : null,
            $data['observedTimestamp'] !== null ? new \DateTimeImmutable($data['observedTimestamp']) : null,
        );
    }

    /**
     * Normalize the LogRecord to an array representation for serialization.
     *
     * @return array{
     *     severity: int,
     *     body: string,
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     timestamp: null|string,
     *     observedTimestamp: null|string
     * }
     */
    public function normalize() : array
    {
        return [
            'severity' => $this->severity->value,
            'body' => $this->body,
            'attributes' => $this->attributes->normalize(),
            'timestamp' => $this->timestamp?->format('c'),
            'observedTimestamp' => $this->observedTimestamp?->format('c'),
        ];
    }

    /**
     * Set an attribute on the log record.
     *
     * Attributes provide additional context about the log event.
     *
     * @param string $key Attribute key
     * @param array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable $value Attribute value
     *
     * @return self New instance with attribute set
     */
    public function setAttribute(string $key, string|int|float|bool|\DateTimeInterface|\Throwable|array $value) : self
    {
        return new self(
            $this->severity,
            $this->body,
            $this->attributes->with($key, $value),
            $this->timestamp,
            $this->observedTimestamp,
        );
    }

    /**
     * Set multiple attributes at once.
     *
     * @param array<string, array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable> $attributes Key-value attribute pairs
     *
     * @return self New instance with attributes set
     */
    public function setAttributes(array $attributes) : self
    {
        return new self(
            $this->severity,
            $this->body,
            $this->attributes->merge(Attributes::create($attributes)),
            $this->timestamp,
            $this->observedTimestamp,
        );
    }

    /**
     * Set the log body (message).
     *
     * The body is the primary content of the log record.
     *
     * @param string $body The log message
     *
     * @return self New instance with body set
     */
    public function setBody(string $body) : self
    {
        return new self(
            $this->severity,
            $body,
            $this->attributes,
            $this->timestamp,
            $this->observedTimestamp,
        );
    }

    /**
     * Record an exception in this log record.
     *
     * This is a convenience method that sets appropriate attributes
     * for the exception (type, message, stacktrace).
     *
     * @param \Throwable $exception The exception to record
     *
     * @return self New instance with exception attributes set
     */
    public function setException(\Throwable $exception) : self
    {
        return $this->setAttributes([
            'exception.type' => $exception::class,
            'exception.message' => $exception->getMessage(),
            'exception.stacktrace' => $exception->getTraceAsString(),
        ]);
    }

    /**
     * Set the observed timestamp for this log record.
     *
     * The observed timestamp is when the log record was observed by the
     * collection system. This is typically set by the Logger or LogProcessor,
     * not by the application code.
     *
     * @param \DateTimeImmutable $observedTimestamp When the log was observed
     *
     * @return self New instance with observed timestamp set
     */
    public function setObservedTimestamp(\DateTimeImmutable $observedTimestamp) : self
    {
        return new self(
            $this->severity,
            $this->body,
            $this->attributes,
            $this->timestamp,
            $observedTimestamp,
        );
    }

    /**
     * Set the severity level.
     *
     * @param Severity $severity The severity level
     *
     * @return self New instance with severity set
     */
    public function setSeverity(Severity $severity) : self
    {
        return new self(
            $severity,
            $this->body,
            $this->attributes,
            $this->timestamp,
            $this->observedTimestamp,
        );
    }

    /**
     * Set the timestamp for this log record.
     *
     * The timestamp represents when the event occurred. If not set,
     * Logger implementations should use the current time.
     *
     * @param \DateTimeImmutable $timestamp When the event occurred
     *
     * @return self New instance with timestamp set
     */
    public function setTimestamp(\DateTimeImmutable $timestamp) : self
    {
        return new self(
            $this->severity,
            $this->body,
            $this->attributes,
            $timestamp,
            $this->observedTimestamp,
        );
    }
}
