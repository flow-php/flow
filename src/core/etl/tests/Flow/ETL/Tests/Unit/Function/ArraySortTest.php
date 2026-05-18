<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_array;
use function json_decode;

final class ArraySortTest extends FlowTestCase
{
    public function test_array_sort_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArraySort function requires non-null array and sort function');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('array')->arraySort()->eval(row(str_entry('array', 'string')), $context);
    }

    public function test_sorting_big_arrays(): void
    {
        static::assertSame(
            ref('array')
                ->arraySort()
                ->eval(
                    row(json_entry(
                        'array',
                        type_array()->assert(json_decode($this->jsonDifferentOrder(), true, 512, JSON_THROW_ON_ERROR)),
                    )),
                    flow_context(),
                ),
            ref('array')
                ->arraySort()
                ->eval(
                    row(json_entry(
                        'array',
                        type_array()->assert(json_decode($this->json(), true, 512, JSON_THROW_ON_ERROR)),
                    )),
                    flow_context(),
                ),
        );
    }

    public function test_sorting_nested_array_using_asort_algo(): void
    {
        static::assertSame(
            [
                'a' => [
                    'g' => 'h',
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                ],
            ],
            ref('array')
                ->arraySort(Sort::asort)
                ->eval(
                    row(json_entry('array', [
                        'a' => [
                            'b' => [
                                'e' => 'f',
                                'c' => 'd',
                            ],
                            'g' => 'h',
                        ],
                    ])),
                    flow_context(),
                ),
        );
    }

    public function test_sorting_nested_associative_array(): void
    {
        static::assertSame(
            [
                'a' => [
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                    'g' => 'h',
                ],
            ],
            ref('array')
                ->arraySort(Sort::ksort)
                ->eval(
                    row(json_entry('array', [
                        'a' => [
                            'g' => 'h',
                            'b' => [
                                'e' => 'f',
                                'c' => 'd',
                            ],
                        ],
                    ])),
                    flow_context(),
                ),
        );
    }

    public function test_sorting_non_array_value(): void
    {
        static::assertNull(ref('array')->arraySort()->eval(row(str_entry('array', 'string')), flow_context()));
    }

    private function json(): string
    {
        return <<<'JSON'
            [
              {
                "asin": "B00PHQB8EE",
                "images": [
                  {
                    "images": [
                      {
                        "link": "https://m.media-amazon.com/images/I/419NipwmTaL.jpg",
                        "width": 190,
                        "height": 500,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/419NipwmTaL._SL75_.jpg",
                        "width": 29,
                        "height": 75,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/61AQupUe5pL.jpg",
                        "width": 418,
                        "height": 1100,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/51XvsWDOBuS.jpg",
                        "width": 500,
                        "height": 279,
                        "variant": "PT01"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/51XvsWDOBuS._SL75_.jpg",
                        "width": 75,
                        "height": 42,
                        "variant": "PT01"
                      }
                    ],
                    "marketplaceId": "ATVPDKIKX0DER"
                  }
                ],
                "synchronized_at": "2023-04-21 11:33:25"
              }
            ]
            JSON;
    }

    private function jsonDifferentOrder(): string
    {
        return <<<'JSON'
            [
              {
                "asin": "B00PHQB8EE",
                "images": [
                  {
                    "images": [
                    {
                        "link": "https://m.media-amazon.com/images/I/51XvsWDOBuS._SL75_.jpg",
                        "width": 75,
                        "height": 42,
                        "variant": "PT01"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/419NipwmTaL.jpg",
                        "width": 190,
                        "height": 500,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/61AQupUe5pL.jpg",
                        "width": 418,
                        "height": 1100,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/419NipwmTaL._SL75_.jpg",
                        "width": 29,
                        "height": 75,
                        "variant": "MAIN"
                      },
                      {
                        "link": "https://m.media-amazon.com/images/I/51XvsWDOBuS.jpg",
                        "width": 500,
                        "height": 279,
                        "variant": "PT01"
                      }
                    ],
                    "marketplaceId": "ATVPDKIKX0DER"
                  }
                ],
                "synchronized_at": "2023-04-21 11:33:25"
              }
            ]
            JSON;
    }
}
