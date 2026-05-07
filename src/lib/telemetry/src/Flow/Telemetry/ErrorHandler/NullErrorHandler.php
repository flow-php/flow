<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Discards every Throwable.
 */
final readonly class NullErrorHandler implements ErrorHandler
{
    public function handle(\Throwable $error) : void
    {
    }
}
