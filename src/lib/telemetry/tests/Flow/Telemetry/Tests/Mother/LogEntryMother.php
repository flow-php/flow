<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tracer\SpanContext;

final class LogEntryMother
{
    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    public static function create(
        string $body,
        Severity $severity,
        array $attributes = [],
        ?SpanContext $spanContext = null,
    ): LogEntry {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity($severity)
                ->setBody($body)
                ->setAttributes($attributes),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable(),
            $spanContext,
        );
    }

    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    public static function deterministic(
        string $body,
        Severity $severity,
        array $attributes = [],
        ?SpanContext $spanContext = null,
    ): LogEntry {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity($severity)
                ->setBody($body)
                ->setAttributes($attributes),
            ResourceMother::full(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            $spanContext,
        );
    }
}
