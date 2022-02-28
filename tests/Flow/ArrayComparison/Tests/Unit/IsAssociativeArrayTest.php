<?php

declare(strict_types=1);

namespace Flow\ArrayComparison\Tests\Unit;

use Flow\ArrayComparison\IsAssociativeArray;
use PHPUnit\Framework\TestCase;

final class IsAssociativeArrayTest extends TestCase
{
    public function collections() : \Generator
    {
        yield [
            ['one', 'two', 'three'],
        ];

        yield [
            [
                ['id' => 1, 'name' => 'one'],
                ['id' => 2, 'name' => 'two'],
                ['id' => 3, 'name' => 'three'],
            ],
        ];
    }

    public function not_collections() : \Generator
    {
        yield [
            ['id' => 1, 'name' => 'one'],
        ];

        yield [
            [
                'item-1' => ['id' => 1, 'name' => 'one'],
                'item-2' => ['id' => 2, 'name' => 'two'],
                'item-3' => ['id' => 3, 'name' => 'three'],
            ],
        ];
    }

    /**
     * @dataProvider collections
     */
    public function test_is_a_collection(array $collection) : void
    {
        $this->assertFalse((new IsAssociativeArray)($collection));
    }

    /**
     * @dataProvider not_collections
     */
    public function test_is_not_a_collection(array $collection) : void
    {
        $this->assertTrue((new IsAssociativeArray)($collection));
    }
}
