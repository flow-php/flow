<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother;

use DateTimeImmutable;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;

use function Flow\Telemetry\DSL\instrumentation_scope;
use function Flow\Telemetry\DSL\resource;

final class LogEntryMother
{
    public static function onChannel(Severity $severity, string $channel): LogEntry
    {
        return new LogEntry(new LogRecord($severity, 'message', [
            'flow.log.channel' => $channel,
        ]), resource(), instrumentation_scope($channel), new DateTimeImmutable());
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    public static function with(Severity $severity, string $body = 'message', array $attributes = []): LogEntry
    {
        return new LogEntry(
            new LogRecord($severity, $body, $attributes),
            resource(),
            instrumentation_scope('test'),
            new DateTimeImmutable(),
        );
    }
}
