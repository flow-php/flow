<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Row\{Reference, References};

final readonly class DropEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = References::init(...$names);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $transformer = fn (Row $row) : Row => $row->remove(...$this->refs);

            $result = $rows->map($transformer);

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
