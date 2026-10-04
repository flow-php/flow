<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Column\Backend;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final readonly class CountingProcessor implements Processor
{
    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $this->schema());
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $count = 0;

        foreach ($rows as $batch) {
            $count += $batch->count();
        }

        yield $this->rows($count, $context->backend());
    }

    /**
     * The one row a count hands out, whether it was counted or known upfront.
     */
    public function rows(int $count, Backend $backend): Rows
    {
        return array_to_rows([['count' => $count]], $this->schema(), $backend);
    }

    public function schema(): Schema
    {
        return schema(int_schema('count'));
    }
}
