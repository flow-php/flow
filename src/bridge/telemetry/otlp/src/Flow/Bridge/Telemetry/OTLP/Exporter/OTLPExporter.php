<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Exporter;

use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Transport\Transport;

/**
 * Exports logs, metrics, and spans to an OTLP endpoint via the configured transport.
 */
final readonly class OTLPExporter implements Exporter
{
    public function __construct(
        private Transport $transport,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
    }

    public function export(Signals $signal) : bool
    {
        if ($signal->count() === 0) {
            return true;
        }

        try {
            $this->transport->send($signal);

            return true;
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [$this->transport];
    }
}
