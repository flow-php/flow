<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetFileSinks;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\RecordingParquetEngine;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class ParquetFileSinkTest extends FlowTestCase
{
    public function test_a_sink_closed_before_its_first_write_never_opens_a_writer(): void
    {
        $engine = new RecordingParquetEngine();

        (new ParquetFileSinks(null, new SchemaConverter(), Compressions::UNCOMPRESSED, Options::default(), $engine))
            ->open(memory_filesystem()->writeTo(path('memory://a.parquet')), new PhpBackend())
            ->close();

        static::assertSame(0, $engine->openedForWrite);
    }

    public function test_the_first_write_opens_one_writer_for_every_write(): void
    {
        $filesystem = memory_filesystem();
        $engine = new RecordingParquetEngine();
        $sink = (new ParquetFileSinks(
            null,
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            $engine,
        ))->open($filesystem->writeTo(path('memory://a.parquet')), new PhpBackend());

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $sink->close();

        static::assertSame(1, $engine->openedForWrite);
        static::assertSame([['id' => 1], ['id' => 2]], ParquetFilesContext::values($filesystem, 'memory://a.parquet'));
    }
}
