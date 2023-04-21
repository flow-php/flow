<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArraySortTest extends TestCase
{
    public function test_sorting_nested_array_using_asort_algo() : void
    {
        $this->assertSame(
            [
                'a' => [
                    'g' => 'h',
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                ],
            ],
            ref('array')->arraySort(\Closure::fromCallable('asort'))->eval(Row::create(
                Entry::array(
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
                )
            ))
        );
    }

    public function test_sorting_nested_associative_array() : void
    {
        $this->assertSame(
            [
                'a' => [
                    'b' => [
                        'c' => 'd',
                        'e' => 'f',
                    ],
                    'g' => 'h',
                ],
            ],
            ref('array')->arraySort()->eval(Row::create(
                Entry::array(
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
                )
            ))
        );
    }

    public function test_sorting_non_array_value() : void
    {
        $this->assertNull(
            ref('array')->arraySort()->eval(Row::create(Entry::str('array', 'string')))
        );
    }
}
