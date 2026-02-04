<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\{ref, row, str_entry};
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Row\Reference;

final readonly class SerializeTransformer implements Transformer
{
    public function __construct(private Reference|string $target, private bool $standalone = false)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $target = $this->target instanceof Reference ? $this->target : ref($this->target);

            $result = $rows->map(
                fn (Row $row) => $this->standalone
                    ? row(str_entry($target->name(), $context->config->serializer()->serialize($row)))
                    : $row->add(str_entry($target->name(), $context->config->serializer()->serialize($row)))
            );

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
