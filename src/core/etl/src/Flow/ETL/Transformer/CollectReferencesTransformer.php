<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class CollectReferencesTransformer implements Transformer
{
    public function __construct(
        private References $references,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            foreach ($rows->schema()->references() as $reference) {
                $this->references->add($reference);
            }

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $rows->count(),
            ]);

            return $rows;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
