<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref, to_memory, type_integer, type_list};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class CastTest extends FlowTestCase
{
    public function test_cast() : void
    {
        df()
            ->read(from_array(
                [
                    ['date' => new \DateTimeImmutable('2023-01-01')],
                ]
            ))
            ->withEntry('date', ref('date')->cast('string'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertEquals(
            [
                ['date' => '2023-01-01T00:00:00+00:00'],
            ],
            $memory->dump()
        );
    }

    public function test_cast_non_deterministic_values() : void
    {
        $row = df()
            ->read(from_array(
                [
                    ['array' => []],
                ]
            ))
            ->withEntry('list_int', ref('array')->cast(type_list(type_integer(), true)))
            ->drop('array')
            ->fetch()->first();

        self::assertEquals(
            type_list(type_integer(), true),
            $row->get('list_int')->type()
        );
    }
}
