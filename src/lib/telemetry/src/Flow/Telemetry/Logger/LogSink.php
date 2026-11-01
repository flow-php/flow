<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * Marker so middleware or a pipeline cannot be wired into a pipeline's sink slot.
 */
interface LogSink extends LogProcessor {}
