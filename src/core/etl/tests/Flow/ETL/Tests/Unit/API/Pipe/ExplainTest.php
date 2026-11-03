<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\explain;
use function Flow\ETL\DSL\from_array;

final class ExplainTest extends FlowTestCase
{
    public function test_explain_draws_the_plan(): void
    {
        static::assertSame(
            "#2 Result\n│  Rows this plan hands out: to the trigger, or to the node reading it\n└─ #1 Read\n      Extractor: ArrayExtractor",
            explain()(df()->read(from_array([['a' => 1], ['a' => 2]])))->toString(),
        );
    }
}
