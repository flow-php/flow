<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\EngineInterface;
use Flow\ETL\Adapter\Seal\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Throwable;

final class SealLoader implements Loader
{
    private int $bulkSize = 100;

    public function __construct(
        private readonly EngineInterface $engine,
        private readonly string $index,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->engine->bulk(
                $this->index,
                (new RowsNormalizer(new EntryNormalizer()))->normalize($rows),
                [],
                $this->bulkSize,
            );

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withBulkSize(int $bulkSize): self
    {
        $this->bulkSize = $bulkSize;

        return $this;
    }
}
