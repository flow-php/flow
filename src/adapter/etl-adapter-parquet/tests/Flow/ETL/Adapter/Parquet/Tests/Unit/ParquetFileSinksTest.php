<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetFileSinks;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class ParquetFileSinksTest extends FlowTestCase
{
    public function test_the_first_conform_fixes_the_schema_every_stream_is_written_under(): void
    {
        $filesystem = memory_filesystem();
        $sinks = new ParquetFileSinks(
            null,
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            new PhpParquetEngine(),
        );
        $first = $sinks->open($filesystem->writeTo(path('memory://a.parquet')), new PhpBackend());
        $second = $sinks->open($filesystem->writeTo(path('memory://b.parquet')), new PhpBackend());

        $first->write(array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))));
        $second->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $first->close();
        $second->close();

        static::assertSame(['id', 'name'], ParquetFilesContext::columnNames($filesystem, 'memory://b.parquet'));
        static::assertSame(
            [['id' => 2, 'name' => null]],
            ParquetFilesContext::values($filesystem, 'memory://b.parquet'),
        );
    }

    public function test_a_declared_schema_is_the_written_one(): void
    {
        $filesystem = memory_filesystem();
        $sink = (new ParquetFileSinks(
            schema(int_schema('id', nullable: true), str_schema('note', nullable: true)),
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            new PhpParquetEngine(),
        ))->open($filesystem->writeTo(path('memory://a.parquet')), new PhpBackend());

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->close();

        static::assertSame(['id', 'note'], ParquetFilesContext::columnNames($filesystem, 'memory://a.parquet'));
    }

    public function test_a_writer_before_the_first_conform_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The Parquet schema is fixed by the first conform()');

        (new ParquetFileSinks(
            null,
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            new PhpParquetEngine(),
        ))->writer(memory_filesystem()->writeTo(path('memory://a.parquet')));
    }
}
