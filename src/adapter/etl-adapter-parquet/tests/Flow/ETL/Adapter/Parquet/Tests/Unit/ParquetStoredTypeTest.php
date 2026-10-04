<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetStoredType;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;

final class ParquetStoredTypeTest extends FlowTestCase
{
    public function test_a_datetime_zone_is_not_stored(): void
    {
        static::assertSame(
            ParquetStoredType::of(type_datetime()->normalize()),
            ParquetStoredType::of(type_datetime('Europe/Warsaw')->normalize()),
        );
    }

    public function test_a_nested_datetime_zone_is_not_stored(): void
    {
        static::assertSame(
            ParquetStoredType::of(
                type_structure([
                    'at' => type_list(type_datetime()),
                    'by' => type_map(type_string(), type_datetime()),
                ])->normalize(),
            ),
            ParquetStoredType::of(
                type_structure([
                    'at' => type_list(type_datetime('America/New_York')),
                    'by' => type_map(type_string(), type_datetime('Asia/Tokyo')),
                ])->normalize(),
            ),
        );
    }

    public function test_other_types_keep_their_shape(): void
    {
        static::assertSame(type_time_zone()->normalize(), ParquetStoredType::of(type_time_zone()->normalize()));
        static::assertNotSame(
            ParquetStoredType::of(type_list(type_integer())->normalize()),
            ParquetStoredType::of(type_list(type_string())->normalize()),
        );
    }
}
