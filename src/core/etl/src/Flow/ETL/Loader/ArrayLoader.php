<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Loader, Rows};

final class ArrayLoader implements Loader
{
    /**
     * @param array<array<mixed>> $array
     */
    public function __construct(private array &$array)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $this->array = \array_merge(
                $this->array,
                $rows->toArray()
            );

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }
}
