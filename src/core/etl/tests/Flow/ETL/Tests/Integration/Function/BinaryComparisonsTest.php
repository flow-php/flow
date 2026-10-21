<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\when;

final class BinaryComparisonsTest extends FlowTestCase
{
    public function test_all_comparisons(): void
    {
        data_frame()
            ->read(from_array([
                ['a' => 100, 'b' => 100, 'c' => 10, 'd' => 'value', 'array' => ['a' => 10, 'b' => 20, 'c' => 30]],
            ]))
            ->withColumn('eq', when(ref('a')->equals(ref('b')), lit(true), lit(false)))
            ->withColumn('ne', when(ref('a')->notEquals(ref('c')), lit(true), lit(false)))
            ->withColumn('gt', when(ref('a')->greaterThan(ref('b')), lit(true), lit(false)))
            ->withColumn('ge', when(ref('a')->greaterThanEqual(ref('b')), lit(true), lit(false)))
            ->withColumn('lt', when(ref('a')->lessThan(ref('b')), lit(true), lit(false)))
            ->withColumn('le', when(ref('a')->lessThanEqual(ref('b')), lit(true), lit(false)))
            ->withColumn('in', when(ref('c')->isIn(ref('array')), lit(true), lit(false)))
            ->withColumn('same', when(ref('a')->same(ref('b')), lit(true), lit(false)))
            ->withColumn('not_same', when(ref('a')->notSame(ref('b')), lit(true), lit(false)))
            ->withColumn('numeric', when(ref('a')->isNumeric(), lit(true), lit(false)))
            ->withColumn('not_numeric', when(ref('a')->isNotNumeric(), lit(true), lit(false)))
            ->withColumn('null', when(ref('d')->isNull(), lit(true), lit(false)))
            ->withColumn('not_null', when(ref('d')->isNotNull(), lit(true), lit(false)))
            ->withColumn('type', when(ref('a')->isType('string'), lit(true), lit(false)))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                [
                    'a' => 100,
                    'b' => 100,
                    'c' => 10,
                    'd' => 'value',
                    'eq' => true,
                    'ne' => true,
                    'gt' => false,
                    'ge' => true,
                    'lt' => false,
                    'le' => true,
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
            $memory->dump(),
        );
    }
}
