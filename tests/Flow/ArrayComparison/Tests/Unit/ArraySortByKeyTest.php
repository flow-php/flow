<?php

declare(strict_types=1);

namespace Flow\ArrayComparison\Tests\Unit;

use Flow\ArrayComparison\ArraySortByKey;
use PHPUnit\Framework\TestCase;

final class ArraySortByKeyTest extends TestCase
{
    /**
     * @dataProvider arrays
     */
    public function test_sorts_array_by_key(array $origin, array $sorted) : void
    {
        // serialize to JSON to be sure that array is sorted exactly as expected

        $this->assertEquals(
            \json_encode($sorted),
            \json_encode((new ArraySortByKey)($origin)),
        );
    }

    public function arrays() : \Generator
    {
        yield 'simple array' => [
            ['name' => 'one', 'priority' => 'high', 'id' => 1, 'color' => 'red', 'active' => true],
            ['active' => true, 'color' => 'red', 'id' => 1, 'name' => 'one', 'priority' => 'high'],
        ];

        yield 'array with address object-like' => [
            [
                'name' => 'one',
                'id' => 1,
                'address' => [
                    'state' => 'NY',
                    'city' => 'New York City',
                    'zipcode' => '25124',
                    'country' => 'US',
                ],
            ],
            [
                'address' => [
                    'city' => 'New York City',
                    'country' => 'US',
                    'state' => 'NY',
                    'zipcode' => '25124',
                ],
                'id' => 1,
                'name' => 'one',
            ],
        ];

        yield 'complex array' => [
            [
                'seller-id' => 1,
                'orders' => [
                    [
                        'order-id' => 2,
                        'items' => [
                            [
                                'promotions' => ['free-shipping', 'black-friday'],
                                'item-id' => 3,
                                'item-name' => 'three',
                            ],
                            [
                                'item-name' => 'four',
                                'item-id' => 4,
                            ],
                        ],
                        'address' => [
                            'origin' => [
                                'state' => 'NY',
                                'city' => 'New York City',
                                'zipcode' => '25124',
                                'country' => 'US',
                            ],
                            'destination' => [
                                'country' => 'US',
                                'zipcode' => '65124',
                                'state' => 'CA',
                                'city' => 'Los Angeles',
                            ],
                        ],
                    ],
                ],
            ],
            [
                'orders' => [
                    [
                        'address' => [
                            'destination' => [
                                'city' => 'Los Angeles',
                                'country' => 'US',
                                'state' => 'CA',
                                'zipcode' => '65124',
                            ],
                            'origin' => [
                                'city' => 'New York City',
                                'country' => 'US',
                                'state' => 'NY',
                                'zipcode' => '25124',
                            ],
                        ],
                        'items' => [
                            [
                                'item-id' => 4,
                                'item-name' => 'four',
                            ],
                            [
                                'item-id' => 3,
                                'item-name' => 'three',
                                'promotions' => ['black-friday', 'free-shipping'],
                            ],
                        ],
                        'order-id' => 2,
                    ],
                ],
                'seller-id' => 1,
            ],
        ];
    }
}
