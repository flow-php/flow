<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\process;
use function Flow\ETL\DSL\schema;

final class ProcessTest extends FlowTestCase
{
    public function test_process_starts_a_frame_from_the_rows(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            process(
                array_to_rows([['a' => 1]], schema(int_schema('a'))),
                array_to_rows([['a' => 2]], schema(int_schema('a'))),
            )(df())
                ->fetch()
                ->toArray(),
        );
    }
}
