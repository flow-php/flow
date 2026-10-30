<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\FlowTestCase;

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
            FileColumnsContext::discovering([
                'year' => false,
            ], partition_types(year: type_integer()))->declare(schema(str_schema('id'))),
        );
    }

    public function test_a_declared_schema_still_beats_a_declared_type(): void
    {
        static::assertEquals(
            schema(str_schema('id'), datetime_schema('year')),
            FileColumnsContext::discovering(['year' => false], partition_types(year: type_integer()))->declare(schema(
                str_schema('id'),
                datetime_schema('year'),
            )),
        );
    }

    public function test_nullability_still_comes_from_path_coverage(): void
    {
        static::assertEquals(
            schema(int_schema('year', nullable: true)),
            FileColumnsContext::discovering(['year' => true], partition_types(year: type_integer()))->declare(schema()),
        );
    }

    public function test_naming_a_column_that_is_not_a_partition_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column "month" is not a partition of this read, discovered partitions: ["year"]',
        );

        FileColumnsContext::discovering(['year' => false], partition_types(month: type_datetime()))->declare(schema());
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

    public function test_normalize_round_trips_every_type(): void
    {
        $types = partition_types(year: type_integer(), day: type_datetime());

        static::assertSame(
            ['year' => type_integer()->normalize(), 'day' => type_datetime()->normalize()],
            $types->normalize(),
        );
        static::assertEquals($types, PartitionTypes::fromArray($types->normalize()));
        static::assertSame([], (new PartitionTypes())->normalize());
    }
}
