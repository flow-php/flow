<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Loader;

use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\ref;

final class PartitioningTest extends FlowTestCase
{
    public function test_none_partitions_by_nothing(): void
    {
        static::assertCount(0, Partitioning::none()->by);
        static::assertFalse(Partitioning::none()->writeColumns);
    }

    public function test_by_takes_one_name_or_one_reference(): void
    {
        static::assertSame(['region'], Partitioning::by('region')->by->names());
        static::assertSame(['region'], Partitioning::by(ref('region'))->by->names());
    }

    public function test_by_takes_a_list_of_names_and_references_in_the_given_order(): void
    {
        static::assertSame(['year', 'month'], Partitioning::by(['year', ref('month')])->by->names());
    }

    public function test_by_an_empty_list_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('partitionBy() needs at least one column, an empty list was given');

        // @mago-ignore analysis:invalid-argument
        Partitioning::by([]);
    }

    public function test_write_columns_is_off_by_default(): void
    {
        static::assertFalse(Partitioning::by('region')->writeColumns);
        static::assertTrue(Partitioning::by('region', writeColumns: true)->writeColumns);
    }

    public function test_normalize_round_trips(): void
    {
        $partitioning = Partitioning::by(['a', 'b'], writeColumns: true);

        static::assertEquals(
            $partitioning,
            Partitioning::fromArray(Json::decode(Json::encode($partitioning->normalize()))),
        );
        static::assertSame(['by' => ['a', 'b'], 'write_columns' => true], $partitioning->normalize());
    }

    public function test_from_array_without_columns_is_none(): void
    {
        static::assertEquals(Partitioning::none(), Partitioning::fromArray(['by' => [], 'write_columns' => true]));
    }

    public function test_none_round_trips(): void
    {
        static::assertEquals(Partitioning::none(), Partitioning::fromArray(Partitioning::none()->normalize()));
        static::assertSame(['by' => [], 'write_columns' => false], Partitioning::none()->normalize());
    }
}
