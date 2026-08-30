<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use DateTimeImmutable;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

final class CastTest extends FlowTestCase
{
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
