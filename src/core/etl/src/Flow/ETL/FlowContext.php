<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Calculator\Calculator;
use Flow\ETL\Config\Telemetry\TelemetryContext;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Function\Functions;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Hydrator;

/**
 * Mutable Flow execution context.
 * It can be modified through the DataFrame.
 */
final class FlowContext
{
    private ErrorHandler $errorHandler;

    private readonly Functions $functions;

    private ?TelemetryContext $telemetryContext = null;

    public function __construct(
        public readonly Config $config,
    ) {
        $this->errorHandler = new ThrowError();
        $this->functions = new Functions(ExecutionMode::LENIENT);
    }

    public function cache(): Cache
    {
        return $this->config->cache->cache;
    }

    public function calculator(): Calculator
    {
        return $this->config->calculator();
    }

    public function entryFactory(): EntryFactory
    {
        return $this->config->entryFactory();
    }

    public function errorHandler(): ErrorHandler
    {
        return $this->errorHandler;
    }

    public function functions(): Functions
    {
        return $this->functions;
    }

    /**
     * @return Hydrator
     */
    public function hydrator(): Hydrator
    {
        return $this->config->hydrator();
    }

    public function setErrorHandler(ErrorHandler $handler): self
    {
        $this->errorHandler = $handler;

        return $this;
    }

    public function telemetry(): TelemetryContext
    {
        return $this->telemetryContext ??= new TelemetryContext(
            $this->config->telemetry->telemetry->logger('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->telemetry->tracer('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->telemetry->meter('flow_php_dataframe', $this->config->version()),
            $this->config->telemetry->options,
        );
    }
}
