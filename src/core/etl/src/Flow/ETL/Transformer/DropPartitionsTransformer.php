<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final readonly class DropPartitionsTransformer implements Transformer
{
    public function __construct(
        private bool $dropPartitionColumns = false,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->isPartitioned() ? $rows->dropPartitions($this->dropPartitionColumns) : $rows;

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (\Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
