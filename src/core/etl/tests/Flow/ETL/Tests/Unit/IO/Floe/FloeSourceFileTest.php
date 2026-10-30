<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Floe\FloeSourceFile;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeSourceFileTest extends FlowTestCase
{
    public function test_statistics_come_from_the_footer_and_close_opens_nothing(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);
        $footer = FloeFilesContext::files($memory)->read(path('memory://parts/country=PL/data.floe'))->footer();
        $file = new FloeSourceFile($footer, new SourceFile(path('memory://parts/country=PL/data.floe')));

        $file->close();

        static::assertEquals(Cardinality::exact($footer->statistics->rows), $file->statistics()->rows);
        static::assertEquals(Cardinality::exact($footer->statistics->byteSize), $file->statistics()->size);
        static::assertSame(0, $counting->readFromCalls);
    }

    public function test_schema_comes_from_the_footer(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        static::assertEquals(
            schema(int_schema('id')),
            (new FloeSourceFile(
                FloeFilesContext::files($memory)->read(path('memory://parts/country=PL/data.floe'))->footer(),
                new SourceFile(path('memory://parts/country=PL/data.floe')),
            ))->schema(),
        );
    }

    public function test_source_returns_the_listed_file(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        static::assertEquals(
            new SourceFile(path('memory://parts/country=PL/data.floe')),
            (new FloeSourceFile(
                FloeFilesContext::files($memory)->read(path('memory://parts/country=PL/data.floe'))->footer(),
                new SourceFile(path('memory://parts/country=PL/data.floe')),
            ))->source(),
        );
    }
}
