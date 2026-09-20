<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
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

        static::assertNull($read->limit());
        static::assertInstanceOf(OnlyFiles::class, $read->pathFilter());
    }

    public function test_extractor_limit_and_path_filter_are_the_values_it_was_built_with(): void
    {
        $extractor = from_array([['id' => 1]]);
        $filter = new RejectingFilter();

        $read = new Read($extractor, 5, $filter);

        static::assertSame($extractor, $read->extractor());
        static::assertSame(5, $read->limit());
        static::assertSame($filter, $read->pathFilter());
    }

    public function test_with_path_filter_composes_with_the_filters_already_held(): void
    {
        $extractor = from_array([['id' => 1]]);
        $read = new Read($extractor, 3);
        $first = new RejectingFilter();
        $second = new RejectingFilter();

        $filtered = $read->withPathFilter($first)->withPathFilter($second);

        static::assertInstanceOf(OnlyFiles::class, $read->pathFilter());
        static::assertEquals(new Filters(new OnlyFiles(), $first, $second), $filtered->pathFilter());
        static::assertSame(3, $filtered->limit());
        static::assertSame($extractor, $filtered->extractor());
    }

    public function test_with_limit_returns_a_new_read_and_leaves_the_original_alone(): void
    {
        $read = (new Read(from_array([['id' => 1]])))->withPathFilter($filter = new OnlyFiles());

        $limited = $read->withLimit(5);

        static::assertNull($read->limit());
        static::assertSame(5, $limited->limit());
        static::assertEquals(new Filters(new OnlyFiles(), $filter), $limited->pathFilter());
    }

    public function test_with_limit_narrows_and_never_widens(): void
    {
        $read = new Read(from_array([['id' => 1]]));

        static::assertSame(10, $read->withLimit(10)->withLimit(100)->limit());
        static::assertSame(10, $read->withLimit(100)->withLimit(10)->limit());
    }

    public function test_with_limit_refuses_zero_or_less(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        (new Read(from_array([['id' => 1]])))->withLimit(0);
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
