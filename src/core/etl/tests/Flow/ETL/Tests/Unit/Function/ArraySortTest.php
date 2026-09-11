<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;
use function json_decode;

final class ArraySortTest extends FlowTestCase
{
    public function test_array_sort_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        ref('array')->arraySort()->eval(row(['array' => 'string']), $context);
    }

    public function test_sorting_big_arrays(): void
    {
        static::assertSame(
            ref('array')
                ->arraySort()
                ->eval(row(['array' => type_array()->assert(json_decode(
                    $this->jsonDifferentOrder(),
                    true,
                    512,
                    JSON_THROW_ON_ERROR,
                ))]), flow_context()),
            ref('array')
                ->arraySort()
                ->eval(row(['array' => type_array()->assert(json_decode(
                    $this->json(),
                    true,
                    512,
                    JSON_THROW_ON_ERROR,
                ))]), flow_context()),
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
                ->eval(row([
                    'array' => [
                        'a' => [
                            'b' => [
                                'e' => 'f',
                                'c' => 'd',
                            ],
                            'g' => 'h',
                        ],
                    ],
                ]), flow_context()),
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
                ->eval(row([
                    'array' => [
                        'a' => [
                            'g' => 'h',
                            'b' => [
                                'e' => 'f',
                                'c' => 'd',
                            ],
                        ],
                    ],
                ]), flow_context()),
        );
    }

    public function test_sorting_non_array_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        ref('array')->arraySort()->eval(row(['array' => 'string']), flow_context());
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

    public function test_ksort_declares_key_sorted_structure_fields(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arraySort(Sort::ksort),
            schema(structure_schema('structure', type_structure(['b' => type_integer(), 'a' => type_integer()]))),
        );

        static::assertSame('structure{a: integer, b: integer}', $resolved->returns()->toString());
    }

    public function test_a_value_sort_keeps_the_declared_field_order(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arraySort(Sort::asort),
            schema(structure_schema('structure', type_structure(['b' => type_integer(), 'a' => type_integer()]))),
        );

        static::assertSame('structure{b: integer, a: integer}', $resolved->returns()->toString());
    }

    public function test_krsort_declares_reverse_key_sorted_structure_fields(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arraySort(Sort::krsort),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_integer()]))),
        );

        static::assertSame('structure{b: integer, a: integer}', $resolved->returns()->toString());
    }

    public function test_ksort_orders_across_the_required_and_optional_buckets(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arraySort(Sort::ksort),
            schema(structure_schema('structure', type_structure([
                'z' => type_integer(),
                'a' => structure_element('a', type_integer(), optional: true),
            ]))),
        );

        static::assertSame('structure{a?: integer, z: integer}', $resolved->returns()->toString());
    }

    public function test_ksort_reproduces_php_key_order_for_numeric_element_names(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arraySort(Sort::ksort),
            schema(structure_schema('structure', type_structure([
                10 => type_integer(),
                9 => type_integer(),
                'b' => type_integer(),
            ]))),
        );

        static::assertSame('structure{9: integer, 10: integer, b: integer}', $resolved->returns()->toString());
    }
}
