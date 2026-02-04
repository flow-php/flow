<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class HitsIntoRowsTransformer implements Transformer
{
    public function __construct(
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $newRows = [];

            foreach ($rows as $row) {
                $entries = [];

                foreach ($row->toArray() as $key => $value) {
                    $entries[] = $context->entryFactory()->create($key, $value);
                }

                $newRows[] = Row::create(...$entries);
            }

            $result = new Rows(...$newRows);

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
