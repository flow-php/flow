<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

/**
 * Unlike {@see SelectEntriesTransformer} a column the row never had stays absent
 * instead of becoming null - pruned rows are spilled to storage, where absent and
 * null are distinct.
 */
final readonly class PruneEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $newRows = [];

            foreach ($rows as $row) {
                $newRowEntries = [];

                foreach ($this->refs as $ref) {
                    try {
                        $newRowEntries[] = $row->get($ref);
                    } catch (InvalidArgumentException) {
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
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
