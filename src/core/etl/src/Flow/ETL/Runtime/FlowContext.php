<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\Telemetry\TelemetryContext;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;

final class FlowContext
{
    private ErrorHandler $errorHandler;

    private ?TelemetryContext $telemetryContext = null;

    public function __construct(
        public readonly Config $config,
    ) {
        $this->errorHandler = new ThrowError();
    }

    public function cache(): RowsStorage
    {
        return $this->config->cache->cache;
    }

    public function errorHandler(): ErrorHandler
    {
        return $this->errorHandler;
    }

    public function backend(): Backend
    {
        return $this->config->backend();
    }

    public function withErrorHandler(ErrorHandler $handler): self
    {
        $new = new self($this->config);
        $new->telemetryContext = $this->telemetry();
        $new->errorHandler = $handler;

        return $new;
    }

    public function telemetry(): TelemetryContext
    {
        return $this->telemetryContext ??= new TelemetryContext(
            $this->config->id(),
            $this->config->name(),
            $this->config->backend(),
            $this->config->telemetry->telemetry->logger('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->telemetry->tracer('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->telemetry->meter('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->options,
        );
    }
}
