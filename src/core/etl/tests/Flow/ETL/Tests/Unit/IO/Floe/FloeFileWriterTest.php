<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\IO\Floe\FloeFileWriterFactory;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeFileWriterTest extends FlowTestCase
{
    public function test_a_writer_closed_before_any_write_leaves_a_file_without_rows_under_its_schema(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://a.floe'));

        (new FloeFileWriterFactory(null, null, new Options()))
            ->writer($stream, schema(int_schema('id')), new PhpBackend())
            ->close();
        $stream->close();

        $file = FloeFilesContext::files($filesystem)->read(path('memory://a.floe'));

        static::assertEquals(schema(int_schema('id')), $file->schema());
        static::assertSame([], iterator_to_array($file->rows()));
    }

    public function test_every_write_lands_in_the_file(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://a.floe'));
        $writer = (new FloeFileWriterFactory(null, null, new Options()))->writer(
            $stream,
            schema(int_schema('id')),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $writer->close();
        $stream->close();

        $rows = [];

        foreach (FloeFilesContext::files($filesystem)->read(path('memory://a.floe'))->rows() as $batch) {
            $rows = [...$rows, ...$batch->toArray()];
        }

        static::assertSame([['id' => 1], ['id' => 2]], $rows);
    }
}
