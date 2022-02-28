<?php

declare(strict_types=1);

namespace Flow\ArrayComparison\Tests\Unit;

use Flow\ArrayComparison\ArrayComparison;
use PHPUnit\Framework\TestCase;

final class ArrayComparisonTest extends TestCase
{
    public function equal_arrays() : \Generator
    {
        yield 'simple arrays' => [
            ['id' => 1, 'name' => 'one', 'color' => 'red'],
            ['name' => 'one', 'color' => 'red', 'id' => 1],
        ];

        yield 'simple arrays with null values' => [
            ['id' => 1, 'name' => 'one', 'color' => 'red', 'description' => null],
            ['name' => 'one', 'color' => 'red', 'id' => 1, 'description' => null],
        ];

        yield 'arrays with datetimes' => [
            ['date' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC')],
            ['date' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC')],
        ];

        yield 'arrays with objects' => [
            ['date' => new \stdClass()],
            ['date' => new \stdClass()],
        ];

        yield 'complex arrays' => [
            [
                'id' => 5,
                'labels' => ['pending', 'risky'],
                'title' => 'Title',
                'address' => [
                    'origin' => [
                        'city' => 'New York City',
                        'country' => 'US',
                    ],
                    'delivery' => [
                        'city' => 'Atlanta City',
                        'country' => 'US',
                    ],
                ],
                'items' => [
                    [
                        'name' => 'One',
                        'item-id' => 1,
                    ],
                    [
                        'name' => 'Two',
                        'item-id' => 2,
                    ],
                    [
                        'item-id' => 3,
                        'name' => 'Three',
                    ],
                ],
                'name' => 'Name',
            ],
            [
                'id' => 5,
                'name' => 'Name',
                'title' => 'Title',
                'items' => [
                    [
                        'name' => 'Two',
                        'item-id' => 2,
                    ],
                    [
                        'name' => 'Three',
                        'item-id' => 3,
                    ],
                    [
                        'item-id' => 1,
                        'name' => 'One',
                    ],
                ],
                'address' => [
                    'delivery' => [
                        'city' => 'Atlanta City',
                        'country' => 'US',
                    ],
                    'origin' => [
                        'city' => 'New York City',
                        'country' => 'US',
                    ],
                ],
                'labels' => ['risky', 'pending'],
            ],
        ];
    }

    public function not_equal_arrays() : \Generator
    {
        yield 'simple arrays' => [
            ['id' => 1, 'name' => 'one', 'color' => 'red'],
            ['name' => 'one', 'color' => 'red', 'id' => 2],
        ];

        yield 'arrays with floats' => [
            ['temp' => 36.00],
            ['temp' => 36],
        ];

        yield 'some data but different key' => [
            [
                'items' => [
                    [
                        'name' => 'One',
                        'item-id' => 1,
                    ],
                    [
                        'name' => 'Two',
                        'item-id' => 2,
                    ],
                ],
            ],
            [
                'receipts' => [
                    [
                        'name' => 'One',
                        'item-id' => 1,
                    ],
                    [
                        'name' => 'Two',
                        'item-id' => 2,
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider equal_arrays
     */
    public function test_equals(array $a, array $b) : void
    {
        $this->assertTrue((new ArrayComparison())->equals($a, $b));
    }

    /**
     * @dataProvider not_equal_arrays
     */
    public function test_not_equals(array $a, array $b) : void
    {
        $this->assertFalse((new ArrayComparison())->equals($a, $b));
    }
}
