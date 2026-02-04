<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Rows, Transformer};
use Flow\ETL\Row\{Reference, References};

final readonly class SelectEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $newRows = [];

            foreach ($rows as $row) {
                $newRowEntries = [];

                foreach ($this->refs as $ref) {
                    try {
                        $newRowEntries[] = $row->get($ref);
                    } catch (\Exception) {
                        $newRowEntries[] = str_entry($ref->name(), null);
                    }
                }
                $newRows[] = row(...$newRowEntries);
            }

            $result = rows(...$newRows);

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
