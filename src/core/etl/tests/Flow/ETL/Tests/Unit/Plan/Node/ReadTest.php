<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class ReadTest extends FlowTestCase
{
    public function test_children_is_empty(): void
    {
        static::assertSame([], (new Read(from_array([['id' => 1]])))->children());
    }

    public function test_with_children_is_a_no_op(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertSame($read, $read->withChildren([]));
    }

    public function test_a_default_read_pushes_no_limit_and_lists_only_files(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertNull($read->scan()->limit);
        static::assertInstanceOf(OnlyFiles::class, $read->scan()->pathFilter);
    }

    public function test_extractor_and_scan_are_the_values_it_was_built_with(): void
    {
        $extractor = from_array([['id' => 1]]);
        $scan = new FileScan(5, new RejectingFilter());

        $read = new Read($extractor, $scan);

        static::assertSame($extractor, $read->extractor());
        static::assertSame($scan, $read->scan());
    }

    public function test_with_path_filter_and_with_filter_push_into_the_scan_and_leave_the_original_alone(): void
    {
        $extractor = PartitionedSourceMother::yearMonth();
        $read = new Read($extractor, new FileScan(3));
        $pathFilter = new RejectingFilter();
        $filter = ref('year')->greaterThan(lit(1));

        $pushed = $read->withPathFilter($pathFilter)->withFilter($filter);

        static::assertEquals(new FileScan(3), $read->scan());
        static::assertEquals(
            (new FileScan(3))
                ->withPathFilter($pathFilter)
                ->withFilter($filter),
            $pushed->scan(),
        );
        static::assertSame($extractor, $pushed->extractor());
    }

    public function test_with_limit_pushes_into_the_scan_of_any_extractor(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertNull($read->scan()->limit);
        static::assertSame(10, $read->withLimit(10)->withLimit(100)->scan()->limit);
    }

    public function test_with_limit_refuses_zero_or_less(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        (new Read(from_array([['id' => 1]])))->withLimit(0);
    }

    public function test_a_filter_is_refused_for_an_extractor_that_is_no_file_extractor(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Only a FileExtractor reads a pushed filter, Flow\ETL\Extractor\ArrayExtractor does not',
        );

        (new Read(from_array([['id' => 1]])))->withFilter(ref('id')->greaterThan(lit(1)));
    }

    public function test_a_path_filter_is_refused_for_an_extractor_that_is_no_file_extractor(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Only a FileExtractor reads a pushed filter, Flow\ETL\Extractor\ArrayExtractor does not',
        );

        (new Read(from_array([['id' => 1]])))->withPathFilter(new RejectingFilter());
    }

    public function test_schema_delegates_to_the_extractor(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new Read(from_array([['id' => 1]], schema(int_schema('id')))))->schema(),
        );
    }

    public function test_declarations(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertSame(RowCount::source, $read->rowCount());
        static::assertSame(Transparency::transparent, $read->transparency());
        static::assertSame(Materialization::streaming, $read->materialization());
        static::assertEquals(Redefined::none(), $read->redefines());
    }
}
