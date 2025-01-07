<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, int_entry, lit};
use function Flow\ETL\DSL\{config, flow_context, from_rows, row, rows};
use Flow\ETL\Pipeline\{LinkedPipeline, SynchronousPipeline};
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class LinkedPipelineTest extends FlowTestCase
{
    public function test_linked_pipelines() : void
    {
        $pipeline = new LinkedPipeline(
            (new SynchronousPipeline(from_rows(rows(row(int_entry('id', 1)), row(int_entry('id', 2))))))->add(new ScalarFunctionTransformer('active', lit(true))),
        );

        self::assertEquals(
            [
                rows(row(int_entry('id', 1), bool_entry('active', true)), row(int_entry('id', 2), bool_entry('active', true))),
            ],
            \iterator_to_array($pipeline->process(flow_context(config())))
        );
    }
}
