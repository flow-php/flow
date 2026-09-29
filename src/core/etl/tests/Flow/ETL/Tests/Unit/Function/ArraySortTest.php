<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_instance_of;
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
        (new FunctionContext($context))->eval(
            ref('array')->arraySort(),
            ['array' => 'string'],
            schema(str_schema('array')),
        );
    }

    public static function one_document_in_two_orders(): Generator
    {
        yield 'as stored' => [<<<'JSON'
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
                JSON];
        yield 'reordered' => [<<<'JSON'
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
                JSON];
    }

    #[DataProvider('one_document_in_two_orders')]
    public function test_sorting_big_arrays(string $json): void
    {
        static::assertSame(
            [[
                '2023-04-21 11:33:25',
                'B00PHQB8EE',
                [[
                    'ATVPDKIKX0DER',
                    [
                        [29,  75,   'MAIN', 'https://m.media-amazon.com/images/I/419NipwmTaL._SL75_.jpg'],
                        [42,  75,   'PT01', 'https://m.media-amazon.com/images/I/51XvsWDOBuS._SL75_.jpg'],
                        [190, 500,  'MAIN', 'https://m.media-amazon.com/images/I/419NipwmTaL.jpg'],
                        [279, 500,  'PT01', 'https://m.media-amazon.com/images/I/51XvsWDOBuS.jpg'],
                        [418, 1100, 'MAIN', 'https://m.media-amazon.com/images/I/61AQupUe5pL.jpg'],
                    ],
                ]],
            ]],
            type_instance_of(Json::class)
                ->assert((new FunctionContext(flow_context()))->eval(
                    ref('array')->arraySort(),
                    ['array' => type_array()->assert(json_decode($json, true, 512, JSON_THROW_ON_ERROR))],
                    schema(json_schema('array')),
                ))
                ->toArray(),
        );
    }

    public function test_sorting_nested_array_using_asort_algo(): void
    {
        // a json operand gives a json column: the sorted array is read back from it
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
            type_instance_of(Json::class)
                ->assert((new FunctionContext(flow_context()))->eval(
                    ref('array')->arraySort(Sort::asort),
                    [
                        'array' => [
                            'a' => [
                                'b' => [
                                    'e' => 'f',
                                    'c' => 'd',
                                ],
                                'g' => 'h',
                            ],
                        ],
                    ],
                    schema(json_schema('array')),
                ))
                ->toArray(),
        );
    }

    public function test_sorting_nested_associative_array(): void
    {
        // a json operand gives a json column: the sorted array is read back from it
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
            type_instance_of(Json::class)
                ->assert((new FunctionContext(flow_context()))->eval(
                    ref('array')->arraySort(Sort::ksort),
                    [
                        'array' => [
                            'a' => [
                                'g' => 'h',
                                'b' => [
                                    'e' => 'f',
                                    'c' => 'd',
                                ],
                            ],
                        ],
                    ],
                    schema(json_schema('array')),
                ))
                ->toArray(),
        );
    }

    public function test_sorting_non_array_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        (new FunctionContext(flow_context()))->eval(
            ref('array')->arraySort(),
            ['array' => 'string'],
            schema(str_schema('array')),
        );
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
