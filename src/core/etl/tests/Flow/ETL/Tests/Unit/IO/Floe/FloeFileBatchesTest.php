<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Floe\FloeFileBatches;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

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
        $file = (new FloeFileBatches(new NoopCodec(), 65536, null))->batches(
            FileReadMother::of(schema(int_schema('id')), FileColumnsContext::discovering(names: []), 10, $filesystem),
            new Criteria(),
            new SourceFile(path('memory://a.floe')),
            schema(int_schema('id')),
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
        $file = (new FloeFileBatches(new NoopCodec(), 65536, null))->batches(
            FileReadMother::of(schema(int_schema('id')), FileColumnsContext::discovering(names: []), 10, $filesystem),
            new Criteria(),
            new SourceFile(path('memory://a.floe')),
            schema(int_schema('id')),
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

        static::assertSame(
            [[['id' => 1, 'name' => null]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new FloeFileBatches(new NoopCodec(), 65536, null))->batches(
                        FileReadMother::of(
                            schema(int_schema('id'), str_schema('name', nullable: true)),
                            FileColumnsContext::discovering(names: []),
                            10,
                            $filesystem,
                        ),
                        new Criteria(),
                        new SourceFile(path('memory://a.floe')),
                        schema(int_schema('id'), str_schema('name', nullable: true)),
                        new PhpBackend(),
                        new ReadWindow(),
                    ),
                    false,
                ),
            ),
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
                new NoopCodec(),
                65536,
                new DerivedSchema(schema(int_schema('id')), 'memory://a.floe'),
            ))->batches(
                FileReadMother::of(
                    schema(int_schema('id')),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(),
                new SourceFile(path('memory://b.floe')),
                schema(int_schema('id')),
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging file must be refused');
        } catch (InferredSchemaException) {
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }
}
