<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\when;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class BinaryComparisonsTest extends TestCase
{
    public function test_all_comparisons() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['a' => 100, 'b' => 100, 'c' => 10, 'd' => 'value', 'array' => ['a' => 10, 'b' => 20, 'c' => 30]],
                    ]
                )
            )
            ->withEntry(
                'eq',
                when(ref('a')->equals(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'neq',
                when(ref('a')->notEquals(ref('c')), lit(true), lit(false))
            )
            ->withEntry(
                'gt',
                when(ref('a')->greaterThan(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'gte',
                when(ref('a')->greaterThanEqual(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'lt',
                when(ref('a')->lessThan(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'lte',
                when(ref('a')->lessThanEqual(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'in',
                when(ref('c')->isIn(ref('array')), lit(true), lit(false))
            )
            ->withEntry(
                'same',
                when(ref('a')->same(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'not_same',
                when(ref('a')->notSame(ref('b')), lit(true), lit(false))
            )
            ->withEntry(
                'numeric',
                when(ref('a')->isNumeric(), lit(true), lit(false))
            )
            ->withEntry(
                'not_numeric',
                when(ref('a')->isNotNumeric(), lit(true), lit(false))
            )
            ->withEntry(
                'null',
                when(ref('d')->isNull(), lit(true), lit(false))
            )
            ->withEntry(
                'not_null',
                when(ref('d')->isNotNull(), lit(true), lit(false))
            )
            ->withEntry(
                'type',
                when(ref('a')->isType(StringEntry::class), lit(true), lit(false))
            )
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                [
                    'a' => 100,
                    'b' => 100,
                    'c' => 10,
                    'd' => 'value',
                    'eq' => true,
                    'neq' => true,
                    'gt' => false,
                    'gte' => true,
                    'lt' => false,
                    'lte' => true,
                    'in' => true,
                    'same' => true,
                    'not_same' => false,
                    'numeric' => true,
                    'not_numeric' => false,
                    'null' => false,
                    'not_null' => true,
                    'type' => false,
                ],
            ],
            $memory->data
        );
    }
}
