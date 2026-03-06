<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class RenameEntryTransformer implements Transformer
{
    public function __construct(private string $from, private string $to)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($this->from === $this->to) {
            return $rows;
        }

        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->map(fn (Row $row) : Row => $row->rename($this->from, $this->to));

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
