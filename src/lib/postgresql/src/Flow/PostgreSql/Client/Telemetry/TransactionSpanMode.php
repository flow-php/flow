<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Controls how database transactions are represented as spans.
 */
enum TransactionSpanMode: string
{
    /**
     * A single long-lived span spanning the whole transaction (from BEGIN until
     * COMMIT/ROLLBACK), with query spans nested underneath it.
     */
    case GROUPED = 'grouped';

    /**
     * A separate short-lived span for each transaction operation (BEGIN, COMMIT,
     * ROLLBACK) that starts and ends within that single call.
     */
    case PER_OPERATION = 'per_operation';

    /**
     * No transaction spans are emitted.
     */
    case OFF = 'off';
}
