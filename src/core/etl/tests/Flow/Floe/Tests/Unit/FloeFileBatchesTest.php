<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeFileBatches;
use Flow\Floe\Tests\Context\FloeFilesContext;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FloeFileBatchesTest extends FlowTestCase
{
    public function test_an_offset_that_covers_the_file_skips_it_and_returns_its_rows(): void
    {
        $filesystem = memory_filesystem();
        FloeFilesContext::writeFiles($filesystem, ['memory://a.floe' => array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))]);
        $file = (new FloeFileBatches(
            $filesystem,
            new NoopCodec(),
            65536,
            FileColumnsContext::discovering(names: []),
            null,
        ))->batches(
            new SourceFile(path('memory://a.floe')),
            schema(int_schema('id')),
            10,
            new PhpBackend(),
            new ReadWindow(3),
        );

        static::assertSame([], iterator_to_array($file, false));
        static::assertSame(3, $file->getReturn());
    }

    public function test_an_offset_inside_the_file_starts_the_read_there_and_is_consumed(): void
    {
        $filesystem = memory_filesystem();
        FloeFilesContext::writeFiles($filesystem, ['memory://a.floe' => array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))]);
        $file = (new FloeFileBatches(
            $filesystem,
            new NoopCodec(),
            65536,
            FileColumnsContext::discovering(names: []),
            null,
        ))->batches(
            new SourceFile(path('memory://a.floe')),
            schema(int_schema('id')),
            10,
            new PhpBackend(),
            new ReadWindow(1),
        );

        static::assertSame(
            [[['id' => 2], ['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
        static::assertSame(1, $file->getReturn());
    }

    public function test_a_file_narrower_than_the_body_is_padded_with_nulls(): void
    {
        $filesystem = memory_filesystem();
        FloeFilesContext::writeFiles($filesystem, ['memory://a.floe' => array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))]);
        $file = (new FloeFileBatches(
            $filesystem,
            new NoopCodec(),
            65536,
            FileColumnsContext::discovering(names: []),
            null,
        ))->batches(
            new SourceFile(path('memory://a.floe')),
            schema(int_schema('id'), str_schema('name', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['id' => 1, 'name' => null]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
    }

    public function test_a_file_that_diverges_from_the_derived_schema_is_refused_and_closed(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        FloeFilesContext::writeFiles($filesystem, ['memory://b.floe' => array_to_rows([[
            'name' => 'x',
        ]], schema(str_schema('name')))]);

        try {
            iterator_to_array((new FloeFileBatches(
                $filesystem,
                new NoopCodec(),
                65536,
                FileColumnsContext::discovering(names: []),
                new DerivedSchema(schema(int_schema('id')), 'memory://a.floe'),
            ))->batches(
                new SourceFile(path('memory://b.floe')),
                schema(int_schema('id')),
                10,
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging file must be refused');
        } catch (InferredSchemaException) {
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }
}
