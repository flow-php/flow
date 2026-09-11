<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\FloeSourceFile;
use Flow\Floe\Tests\Context\FloeEngineContext;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeSourceFileTest extends FlowTestCase
{
    public function test_close_releases_the_reader(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeEngineContext::writePartitionedFiles($memory);

        (new FloeSourceFile(
            FloeEngineContext::phpReader($counting)->read(path('memory://parts/country=PL/data.floe')),
            new SourceFile(path('memory://parts/country=PL/data.floe')),
        ))->close();

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_schema_comes_from_the_footer(): void
    {
        $memory = memory_filesystem();
        FloeEngineContext::writePartitionedFiles($memory);

        static::assertEquals(
            schema(int_schema('id')),
            (new FloeSourceFile(
                FloeEngineContext::phpReader($memory)->read(path('memory://parts/country=PL/data.floe')),
                new SourceFile(path('memory://parts/country=PL/data.floe')),
            ))->schema(),
        );
    }

    public function test_source_returns_the_listed_file(): void
    {
        $memory = memory_filesystem();
        FloeEngineContext::writePartitionedFiles($memory);

        static::assertEquals(
            new SourceFile(path('memory://parts/country=PL/data.floe')),
            (new FloeSourceFile(
                FloeEngineContext::phpReader($memory)->read(path('memory://parts/country=PL/data.floe')),
                new SourceFile(path('memory://parts/country=PL/data.floe')),
            ))->source(),
        );
    }
}
