<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use function array_filter;
use function count;
use function sprintf;

/**
 * Raised when one or more batches failed on the primary transport.
 *
 * Each entry pairs the primary failure with the failover outcome:
 * - `failover` is null when the failover transport accepted the batch (data preserved).
 * - `failover` is a Throwable when the failover transport also rejected the batch
 *   (data lost; the only case the operator must escalate as data loss).
 */
final class FailoverTransportException extends TransportException
{
    /**
     * @param non-empty-list<array{primary: \Throwable, failover: null|\Throwable}> $failures
     */
    public function __construct(
        public readonly array $failures,
    ) {
        $count = count($failures);
        $first = $failures[0];
        $absorbed = count(array_filter($failures, static fn(array $f): bool => $f['failover'] === null));
        $lost = $count - $absorbed;

        $message = sprintf(
            'OTLP transport failover: %d primary failure(s) (%d absorbed by failover, %d lost); first primary error: %s%s',
            $count,
            $absorbed,
            $lost,
            $first['primary']->getMessage(),
            $first['failover'] !== null ? sprintf('; first failover error: %s', $first['failover']->getMessage()) : '',
        );

        parent::__construct($message, 0, $first['primary']);
    }
}
