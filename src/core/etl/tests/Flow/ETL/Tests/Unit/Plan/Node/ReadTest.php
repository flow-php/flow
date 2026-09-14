<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Extractor\Scan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

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

    public function test_extractor_and_scan_are_the_values_it_was_built_with(): void
    {
        $extractor = from_array([['id' => 1]]);
        $scan = new Scan(limit: 5);

        $read = new Read($extractor, $scan);

        static::assertSame($extractor, $read->extractor());
        static::assertSame($scan, $read->scan());
    }

    public function test_with_path_filter_returns_a_new_read_and_leaves_the_original_alone(): void
    {
        $extractor = from_array([['id' => 1]]);
        $read = new Read($extractor);
        $filter = new OnlyFiles();

        $filtered = $read->withPathFilter($filter);

        static::assertNotSame($read, $filtered);
        static::assertInstanceOf(OnlyFiles::class, $read->scan()->pathFilter);
        static::assertEquals(new Filters(new OnlyFiles(), $filter), $filtered->scan()->pathFilter);
        static::assertSame($extractor, $filtered->extractor());
    }

    public function test_with_limit_returns_a_new_read_and_leaves_the_original_alone(): void
    {
        $read = (new Read(from_array([['id' => 1]])))->withPathFilter($filter = new OnlyFiles());

        $limited = $read->withLimit(5);

        static::assertNotSame($read, $limited);
        static::assertNull($read->scan()->limit);
        static::assertSame(5, $limited->scan()->limit);
        static::assertEquals(new Filters(new OnlyFiles(), $filter), $limited->scan()->pathFilter);
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
