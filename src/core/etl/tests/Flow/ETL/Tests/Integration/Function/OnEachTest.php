<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref, type_string};
use PHPUnit\Framework\TestCase;

final class OnEachTest extends TestCase
{
    public function test_on_each_function() : void
    {
        $results = df()
            ->read(from_array([
                ['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]],
                ['array' => ['f' => 1, 'g' => 2.3, 'h' => 3, 'i' => 4, 'j' => null]],
            ]))
            ->withEntry('array', ref('array')->onEach(ref('element')->cast(type_string())))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['array' => ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5']],
                ['array' => ['f' => '1', 'g' => '2.3', 'h' => '3', 'i' => '4', 'j' => null]],
            ],
            $results
        );
    }
}
