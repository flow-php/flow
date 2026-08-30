<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

use function array_key_exists;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

/**
 * Unlike {@see SelectEntriesTransformer}, which throws when a reference is not declared by the
 * schema, prune keeps what is there and silently skips absent references - pruned rows are spilled
 * to storage, where the pruning is opportunistic rather than a contract.
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
            $present = [];

            foreach ($this->refs as $ref) {
                if ($rows->schema()->findDefinition($ref) !== null) {
                    $present[] = $ref;
                }
            }

            $schema = $rows->schema()->keep(...$present)->reorder(...$present);
            $newRows = [];

            foreach ($rows as $row) {
                $values = $row->values();
                $pruned = [];

                foreach ($this->refs as $ref) {
                    if (array_key_exists($ref->name(), $values)) {
                        $pruned[$ref->name()] = $values[$ref->name()];
                    }
                }

                $newRows[] = row($pruned);
            }

            $result = rows($schema, ...$newRows);

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
