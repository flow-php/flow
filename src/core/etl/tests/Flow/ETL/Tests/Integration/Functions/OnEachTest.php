<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\Types\DSL\type_string;

final class OnEachTest extends FlowTestCase
{
    public function test_on_each_function(): void
    {
        static::assertEquals(
            [
                ['array' => ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5']],
                // an optional and nullable structure element given null reads back absent (ruling 4)
                ['array' => ['f' => '1', 'g' => '2.3', 'h' => '3', 'i' => '4']],
            ],
            df()
                ->read(from_array([
                    ['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]],
                    ['array' => ['f' => 1, 'g' => 2.3, 'h' => 3, 'i' => 4, 'j' => null]],
                ]))
                ->withColumn('array', ref('array')->onEach(optional(ref('element')->cast(type_string()))))
                ->fetch()
                ->toArray(),
        );
    }
}
