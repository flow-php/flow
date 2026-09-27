<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Throwable;

use function Flow\ETL\DSL\int_schema;
use function range;

final class AddRowIndexTransformer implements Stateful
{
    private int $index;

    public function __construct(
        private readonly string $indexColumn,
        private readonly StartFrom $startFrom,
    ) {
        $this->index = $startFrom === StartFrom::ZERO ? 0 : 1;
    }

    public function fresh(): self
    {
        return new self($this->indexColumn, $this->startFrom);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input->add(int_schema($this->indexColumn)));
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $column = $context->backend()->builder(int_schema($this->indexColumn));

            if ($rows->count() > 0) {
                $column->appendMany(range($this->index, $this->index + $rows->count() - 1));
                $this->index += $rows->count();
            }

            $result = $rows->withColumns($rows->schema()->add(int_schema($this->indexColumn)), [
                $this->indexColumn => $column->finish(),
            ]);

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
