<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\enum_name;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

final class EnumNameTest extends FlowTestCase
{
    public function test_enum_name_produces_string_entry_for_int_backed_enum(): void
    {
        static::assertTrue(type_equals(
            type_string(),
            data_frame()
                ->read(from_rows(rows(
                    schema(enum_schema('e', BackedIntEnum::class)),
                    row(['e' => BackedIntEnum::one]),
                )))
                ->withEntry('code', enum_name(ref('e')))
                ->schema()
                ->get('code')
                ->type(),
        ));
    }

    public function test_enum_name_writes_null_for_null_enum_in_permissive_mode(): void
    {
        data_frame()
            ->read(from_rows(rows(
                schema(enum_schema('e', BackedIntEnum::class, nullable: true)),
                row(['e' => BackedIntEnum::one]),
                row(['e' => null]),
            )))
            ->withEntry('code', optional(enum_name(ref('e'))))
            ->select('code')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['code' => 'one'], ['code' => null]], $memory->dump());
    }

    public function test_enum_name_writes_case_names(): void
    {
        data_frame()
            ->read(from_rows(rows(
                schema(enum_schema('e', BackedIntEnum::class)),
                row(['e' => BackedIntEnum::one]),
                row(['e' => BackedIntEnum::two]),
            )))
            ->withEntry('code', enum_name(ref('e')))
            ->select('code')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame([['code' => 'one'], ['code' => 'two']], $memory->dump());
    }
}
