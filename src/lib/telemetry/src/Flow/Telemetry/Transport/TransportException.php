<?php

declare(strict_types=1);

namespace Flow\Telemetry\Transport;

/**
 * Exception thrown when a transport operation fails.
 *
 * This includes network errors, serialization failures, or backend rejections.
 */
final class TransportException extends \RuntimeException
{
}
