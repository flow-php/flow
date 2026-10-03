<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

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

    public function bind(Schema $input): BoundStep
    {
        $present = [];

        foreach ($this->refs as $ref) {
            if ($input->findDefinition($ref) !== null) {
                $present[] = $ref;
            }
        }

        return new BoundStep($this, $input->keep(...$present)->reorder(...$present));
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
            $result = $rows->project($schema, $context->backend());

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
