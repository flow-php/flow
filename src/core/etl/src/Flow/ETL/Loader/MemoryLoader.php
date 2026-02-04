<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Memory\Memory;

final readonly class MemoryLoader implements Loader
{
    public function __construct(private Memory $memory)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $this->memory->save($rows->toArray());

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }
}
