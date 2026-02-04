<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\row;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Transformer\OrderEntries\Comparator;

final readonly class OrderEntriesTransformer implements Transformer
{
    public function __construct(private Comparator $comparator)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->map(function (Row $row) : Row {
                $entries = $row->entries()->all();

                usort($entries, fn ($left, $right) => $this->comparator->compare($left, $right));

                return row(...$entries);
            });

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
