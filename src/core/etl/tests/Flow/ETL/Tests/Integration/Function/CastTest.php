<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use DateTimeImmutable;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\cast;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class CastTest extends FlowTestCase
{
    public function test_cast_of_an_absent_nullsafe_path_to_an_optional_target_is_null(): void
    {
        $rows = df()
            ->read(from_array([
                ['record' => ['deleteTime' => '2026-09-01T00:00:00Z']],
                ['record' => []],
            ], schema(structure_schema('record', type_structure([
                'deleteTime' => structure_element('deleteTime', type_string(), optional: true),
            ])))))
            ->withEntry('delete_time', cast(array_get(ref('record'), '?deleteTime'), type_optional(type_datetime())))
            ->drop('record')
            ->fetch();

        static::assertEquals(
            [['delete_time' => new DateTimeImmutable('2026-09-01T00:00:00Z')], ['delete_time' => null]],
            $rows->toArray(),
        );
        static::assertTrue($rows->schema()->get('delete_time')->isNullable());
    }

    public function test_cast(): void
    {
        df()
            ->read(from_array([
                ['date' => new DateTimeImmutable('2023-01-01')],
            ]))
            ->withEntry('date', ref('date')->cast('string'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertEquals(
            [
                ['date' => '2023-01-01T00:00:00+00:00'],
            ],
            $memory->dump(),
        );
    }

    public function test_cast_to_array(): void
    {
        $rows = df()
            ->read(from_array([['a' => '[1,2,3]']]))
            ->withEntry('b', ref('a')->cast('array'))
            ->fetch();

        // Cast declares type_array (a json column) and the bind enforces that declaration.
        $json = $rows->first()->get('b');

        static::assertInstanceOf(Json::class, $json);
        static::assertSame([1, 2, 3], $json->toArray());
    }

    public function test_cast_non_deterministic_values(): void
    {
        $row = df()
            ->read(from_array([
                ['array' => []],
            ]))
            ->withEntry('list_int', ref('array')->cast(type_optional(type_list(type_integer()))))
            ->drop('array')
            ->fetch()
            ->first();

        static::assertSame([], $row->get('list_int'));
    }
}
