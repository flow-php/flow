<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Function\All;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\Filters;
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

    public function test_with_path_filter_composes_with_the_filters_already_held(): void
    {
        $extractor = from_array([['id' => 1]]);
        $read = new Read($extractor, new FileScan(3));
        $first = new RejectingFilter();
        $second = new RejectingFilter();

        $filtered = $read->withPathFilter($first)->withPathFilter($second);

        static::assertInstanceOf(OnlyFiles::class, $read->scan()->pathFilter);
        static::assertEquals(new Filters(new OnlyFiles(), $first, $second), $filtered->scan()->pathFilter);
        static::assertSame(3, $filtered->scan()->limit);
        static::assertSame($extractor, $filtered->extractor());
    }

    public function test_with_limit_returns_a_new_read_and_leaves_the_original_alone(): void
    {
        $read = (new Read(from_array([['id' => 1]])))->withPathFilter($filter = new OnlyFiles());

        $limited = $read->withLimit(5);

        static::assertNull($read->scan()->limit);
        static::assertSame(5, $limited->scan()->limit);
        static::assertEquals(new Filters(new OnlyFiles(), $filter), $limited->scan()->pathFilter);
    }

    public function test_with_limit_narrows_and_never_widens(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertSame(10, $read->withLimit(10)->withLimit(100)->scan()->limit);
        static::assertSame(10, $read->withLimit(100)->withLimit(10)->scan()->limit);
    }

    public function test_with_limit_refuses_zero_or_less(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        (new Read(from_array([['id' => 1]])))->withLimit(0);
    }

    public function test_with_filter_pushes_the_first_predicate_as_is(): void
    {
        $predicate = ref('id')->greaterThan(lit(1));

        static::assertSame($predicate, (new Read(from_array([['id' => 1]])))->withFilter($predicate)->scan()->filter);
    }

    public function test_with_filter_combines_with_all(): void
    {
        $read = new Read(from_array([['id' => 1]]));
        $first = ref('id')->greaterThan(lit(1));
        $second = ref('id')->lessThan(lit(10));
        $third = ref('id')->notEquals(lit(5));

        $filtered = $read
            ->withPathFilter($pathFilter = new RejectingFilter())
            ->withFilter($first)
            ->withFilter($second);

        static::assertNull($read->scan()->filter);
        static::assertEquals(new All($first, $second), $filtered->scan()->filter);
        static::assertEquals(new All($first, $second, $third), $filtered->withFilter($third)->scan()->filter);
        static::assertEquals(new Filters(new OnlyFiles(), $pathFilter), $filtered->scan()->pathFilter);
    }

    public function test_with_filter_after_a_limit_keeps_the_limit(): void
    {
        $predicate = ref('id')->greaterThan(lit(1));
        $scan = (new Read(from_array([['id' => 1]])))
            ->withLimit(5)
            ->withFilter($predicate)
            ->scan();

        static::assertSame(5, $scan->limit);
        static::assertSame($predicate, $scan->filter);
    }

    public function test_with_limit_after_a_filter_keeps_the_filter(): void
    {
        $predicate = ref('id')->greaterThan(lit(1));
        $scan = (new Read(from_array([['id' => 1]])))
            ->withFilter($predicate)
            ->withLimit(5)
            ->scan();

        static::assertSame(5, $scan->limit);
        static::assertSame($predicate, $scan->filter);
    }

    public function test_with_path_filter_keeps_the_pushed_filter(): void
    {
        $predicate = ref('id')->greaterThan(lit(1));

        static::assertSame(
            $predicate,
            (new Read(from_array([['id' => 1]])))
                ->withFilter($predicate)
                ->withPathFilter(new RejectingFilter())
                ->scan()
                ->filter,
        );
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
