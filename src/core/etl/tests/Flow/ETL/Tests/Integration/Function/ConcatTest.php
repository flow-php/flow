<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_get, concat, from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ConcatTest extends FlowTestCase
{
    public function test_concat_on_non_string_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('concat', concat(ref('id'), lit(null)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'concat' => null],
                ['id' => 2, 'concat' => null],
            ],
            $memory->dump()
        );
    }

    public function test_concat_on_stringable_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['field' => 'value']],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('concat', concat(ref('id'), '-', array_get(ref('array'), 'field')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'concat' => '1-value'],
                ['id' => 2, 'concat' => null],
            ],
            $memory->dump()
        );
    }
}
