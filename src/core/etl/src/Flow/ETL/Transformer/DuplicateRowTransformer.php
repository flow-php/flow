<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\WithEntry;
use Throwable;

use function Flow\ETL\DSL\rows;

final readonly class DuplicateRowTransformer implements Transformer
{
    /**
     * @var array<WithEntry>
     */
    private array $entries;

    /**
     * @param mixed $condition
     * @param WithEntry ...$entries
     */
    public function __construct(
        private mixed $condition,
        WithEntry ...$entries,
    ) {
        $this->entries = $entries;
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $inputRowCount = $rows->count();

        $context->telemetry()->transformationStarted($this);

        try {
            $duplicatedRows = rows();

            foreach ($rows->all() as $row) {
                $condition = (new Parameter($this->condition))->asBoolean($row, $context);

                if ($condition) {
                    $duplicatedRow = rows($row);

                    foreach ($this->entries as $entry) {
                        $duplicatedRow = (new ScalarFunctionTransformer($entry->name, $entry->function))->transform(
                            $duplicatedRow,
                            $context,
                        );
                    }

                    $duplicatedRows = $duplicatedRows->merge($duplicatedRow);
                }
            }

            if ($duplicatedRows->count()) {
                $rows = $rows->merge($duplicatedRows);
            }

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $inputRowCount,
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $rows->count(),
            ]);

            return $rows;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
