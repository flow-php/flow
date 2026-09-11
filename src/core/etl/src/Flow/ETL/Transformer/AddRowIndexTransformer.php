<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\ETL\DSL\int_schema;

final class AddRowIndexTransformer implements Transformer
{
    private int $index;

    public function __construct(
        private readonly string $indexColumn,
        StartFrom $startFrom,
    ) {
        $this->index = $startFrom === StartFrom::ZERO ? 0 : 1;
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input->add(int_schema($this->indexColumn)));
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $indexed = [];

            foreach ($rows->all() as $row) {
                $indexed[] = new Row([...$row->values(), $this->indexColumn => $this->index]);
                $this->index++;
            }

            $result = new Rows($rows->schema()->add(int_schema($this->indexColumn)), ...$indexed);

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
