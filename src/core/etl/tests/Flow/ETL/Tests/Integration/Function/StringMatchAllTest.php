<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_string_match_all_multiple_rows() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'price: 19.99 and 5.50'],
                        ['text' => 'no prices here'],
                        ['text' => 'total: 100.00'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\d+\.\d+/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'price: 19.99 and 5.50', 'match_results' => [['19.99'], ['5.50']]],
                ['text' => 'no prices here', 'match_results' => []],
                ['text' => 'total: 100.00', 'match_results' => [['100.00']]],
            ],
            $memory->dump()
        );
    }
}
