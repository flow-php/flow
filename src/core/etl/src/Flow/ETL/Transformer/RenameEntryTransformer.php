<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\RowProjection;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class RenameEntryTransformer implements Transformer
{
    public function __construct(
        private string $from,
        private string $to,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        if ($this->from === $this->to) {
            return $rows;
        }

        $context->telemetry()->transformationStarted($this);

        try {
            $projection = new RowProjection();
            $renames = [$this->from => $this->to];

            $result = $rows->map(
                $rows->schema()->rename($this->from, $this->to),
                static fn(Row $row): Row => $projection->rename($row, $renames),
            );

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
