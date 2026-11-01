<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

use Throwable;

/**
 * Implementations must never throw: the SDK never throws to user code (OTel).
 *
 * @see https://opentelemetry.io/docs/specs/otel/error-handling/
 */
interface ErrorHandler
{
    public function handle(Throwable $error): void;
}
