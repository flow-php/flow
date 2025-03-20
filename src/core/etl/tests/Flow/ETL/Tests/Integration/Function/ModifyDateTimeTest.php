<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_get, concat, from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ModifyDateTimeTest extends FlowTestCase
{
    public function test_concat_on_non_string_value() : void
    {
        $rows = (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'created_at' => new \DateTimeImmutable('2025-01-01')],
                        ['id' => 2, 'created_at' => new \DateTimeImmutable('2025-02-01')],
                    ]
                )
            )
            ->withEntry('expiration_at', ref('created_at')->modifyDateTime('last day of this month'))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['id' => 1, 'created_at' => new \DateTimeImmutable('2025-01-01'), 'expiration_at' => new \DateTimeImmutable('2025-01-31')],
                ['id' => 2, 'created_at' => new \DateTimeImmutable('2025-02-01'), 'expiration_at' => new \DateTimeImmutable('2025-02-28')],
            ],
            $rows,
        );
    }

    public function test_concat_on_nulls() : void
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
            ->withEntry('concat', concat(lit(null), lit(null)))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'concat' => ''],
                ['id' => 2, 'concat' => ''],
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
                ['id' => 2, 'concat' => '2-'],
            ],
            $memory->dump()
        );
    }
}
