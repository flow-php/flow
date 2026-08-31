<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\Types\Type\AutoCaster;
use Throwable;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;

final readonly class AutoCastTransformer implements Transformer
{
    public function __construct(
        private AutoCaster $caster,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = array_to_rows(array_map(fn(Row $row): array => array_map(
                $this->caster->cast(...),
                $row->values(),
            ), $rows->all()), $context->hydrator());

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
