<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Extractor\PartitionTypes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\MemoryFilesystem;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;

final class PartitionTypesTest extends FlowTestCase
{
    public function test_a_declared_type_beats_the_string_default(): void
    {
        static::assertEquals(
            schema(str_schema('id'), int_schema('year')),
            (new PartitionColumns(new MemoryFilesystem()))->declare(
                schema(str_schema('id')),
                ['year' => false],
                partition_types(year: type_integer()),
            ),
        );
    }

    public function test_a_declared_schema_still_beats_a_declared_type(): void
    {
        static::assertEquals(
            schema(str_schema('id'), datetime_schema('year')),
            (new PartitionColumns(new MemoryFilesystem()))->declare(
                schema(str_schema('id'), datetime_schema('year')),
                ['year' => false],
                partition_types(year: type_integer()),
            ),
        );
    }

    public function test_nullability_still_comes_from_path_coverage(): void
    {
        static::assertEquals(
            schema(int_schema('year', nullable: true)),
            (new PartitionColumns(new MemoryFilesystem()))->declare(
                schema(),
                ['year' => true],
                partition_types(year: type_integer()),
            ),
        );
    }

    public function test_naming_a_column_that_is_not_a_partition_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column "month" is not a partition of this read, discovered partitions: ["year"]',
        );

        (new PartitionColumns(new MemoryFilesystem()))->declare(
            schema(),
            ['year' => false],
            partition_types(month: type_datetime()),
        );
    }

    public function test_an_undeclared_name_is_refused_by_get(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No partition type declared for "month"');

        (new PartitionTypes())->get('month');
    }

    public function test_the_dsl_delegates_without_normalising(): void
    {
        static::assertEquals(new PartitionTypes(['year' => type_integer()]), partition_types(year: type_integer()));
    }
}
