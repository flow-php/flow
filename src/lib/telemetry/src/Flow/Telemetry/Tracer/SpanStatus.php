<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * Represents the status of a span.
 *
 * SpanStatus combines a status code with an optional description message.
 * According to OpenTelemetry specification, descriptions are only meaningful
 * for ERROR status.
 *
 * Example usage:
 * ```php
 * $status = SpanStatus::ok();
 * $status = SpanStatus::error('Connection timeout');
 * ```
 */
final readonly class SpanStatus
{
    public function __construct(
        public SpanStatusCode $code,
        public ?string $description = null,
    ) {
    }

    /**
     * Create an ERROR status with an optional description.
     */
    public static function error(?string $description = null) : self
    {
        return new self(SpanStatusCode::ERROR, $description);
    }

    /**
     * Create a SpanStatus from a normalized array representation.
     *
     * @param array{code: int, description: null|string} $data Normalized SpanStatus data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            SpanStatusCode::from($data['code']),
            $data['description'],
        );
    }

    /**
     * Create an OK status.
     */
    public static function ok() : self
    {
        return new self(SpanStatusCode::OK);
    }

    /**
     * Create an UNSET status (default).
     */
    public static function unset() : self
    {
        return new self(SpanStatusCode::UNSET);
    }

    /**
     * Check if the status is ERROR.
     */
    public function isError() : bool
    {
        return $this->code === SpanStatusCode::ERROR;
    }

    /**
     * Check if the status is OK.
     */
    public function isOk() : bool
    {
        return $this->code === SpanStatusCode::OK;
    }

    /**
     * Check if the status is UNSET.
     */
    public function isUnset() : bool
    {
        return $this->code === SpanStatusCode::UNSET;
    }

    /**
     * Normalize the SpanStatus to an array representation for serialization.
     *
     * @return array{code: int, description: null|string}
     */
    public function normalize() : array
    {
        return [
            'code' => $this->code->value,
            'description' => $this->description,
        ];
    }
}
