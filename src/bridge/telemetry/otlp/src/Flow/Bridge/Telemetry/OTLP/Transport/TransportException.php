<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use RuntimeException;

/**
 * Exception thrown when an OTLP transport operation fails.
 *
 * This includes network errors, serialization failures, or backend rejections.
 */
class TransportException extends RuntimeException {}
