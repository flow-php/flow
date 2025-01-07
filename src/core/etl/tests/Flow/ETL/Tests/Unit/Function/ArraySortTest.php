<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{json_entry, ref, str_entry};
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Tests\FlowTestCase;

final class ArraySortTest extends FlowTestCase
{
    public function test_sorting_big_arrays() : void
    {
        self::assertSame(
            ref('array')->arraySort()->eval(row(json_entry('array', \json_decode($this->jsonDifferentOrder(), true, 512, JSON_THROW_ON_ERROR)))),
            ref('array')->arraySort()->eval(row(json_entry('array', \json_decode($this->json(), true, 512, JSON_THROW_ON_ERROR))))
        );
    }

    public function test_sorting_nested_array_using_asort_algo() : void
    {
        self::assertSame(
            [
                'a' => [
                    'g' => 'h',
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                ],
            ],
            ref('array')->arraySort(Sort::asort)->eval(row(json_entry(
                'array',
                [
                    'a' => [
                        'b' => [
                            'e' => 'f',
                            'c' => 'd',
                        ],
                        'g' => 'h',
                    ],
                ]
            )))
        );
    }

    public function test_sorting_nested_associative_array() : void
    {
        self::assertSame(
            [
                'a' => [
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                    'g' => 'h',
                ],
            ],
            ref('array')->arraySort(Sort::ksort)->eval(row(json_entry(
                'array',
                [
                    'a' => [
                        'g' => 'h',
                        'b' => [
                            'e' => 'f',
                            'c' => 'd',
                        ],

                    ],
                ]
            )))
        );
    }

    public function test_sorting_non_array_value() : void
    {
        self::assertNull(
            ref('array')->arraySort()->eval(row(str_entry('array', 'string')))
        );
    }

    private function json() : string
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

    private function jsonDifferentOrder() : string
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
