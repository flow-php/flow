<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class StringMatchTest extends FlowTestCase
{
    public function test_string_match_multiple_rows(): void
    {
        data_frame()
            ->read(from_array([
                ['text' => 'hello world'],
                ['text' => 'foo bar'],
                ['text' => 'hello universe'],
            ]))
            ->withColumn('match_result', ref('text')->stringMatch(lit('/hello/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['text' => 'hello world', 'match_result' => ['hello']],
                ['text' => 'foo bar', 'match_result' => null],
                ['text' => 'hello universe', 'match_result' => ['hello']],
            ],
            $memory->dump(),
        );
    }
}
