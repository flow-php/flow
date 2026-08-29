<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
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
            // An empty batch has no schema to bind against.
            if (!$rows->count()) {
                $context->telemetry()->transformationCompleted($this, [
                    TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => 0,
                    TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => 0,
                ]);

                return $rows;
            }

            /** @var mixed $condition */
            $condition = $this->condition;

            if ($condition instanceof ScalarFunction) {
                $schema = $rows->schema();
                $resolver = new ReferenceResolver();
                $condition = $resolver->resolve($condition, $schema);
                $resolver->assertResolved($condition, $schema);
            }

            $duplicated = [];

            foreach ($rows->all() as $row) {
                if ((new Parameter($condition))->asBoolean($row, $context) ?? false) {
                    $duplicated[] = $row;
                }
            }

            // The maps inside ScalarFunctionTransformer are per-row and stateless, so applying each
            // entry once over all duplicated rows is equivalent to applying it per duplicated row.
            if ($duplicated !== []) {
                $duplicatedRows = rows(...$duplicated);

                foreach ($this->entries as $entry) {
                    $duplicatedRows = (new ScalarFunctionTransformer($entry->name, $entry->function))->transform(
                        $duplicatedRows,
                        $context,
                    );
                }

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
