<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

/**
 * Exception thrown when an OTLP transport operation fails.
 *
 * This includes network errors, serialization failures, or backend rejections.
 */
final class TransportException extends \RuntimeException
{
}
