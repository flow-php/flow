<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetEncoder;
use Flow\ETL\Adapter\Parquet\PhpParquetOpenSink;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
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

final class PhpParquetOpenSinkTest extends FlowTestCase
{
    public function test_written_batches_read_back_equal(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $parquetSchema = (new SchemaConverter())->toParquet($schema);
        $sink = new PhpParquetOpenSink(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://out.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetEncoder($parquetSchema),
        );

        $sink->write(array_to_rows([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $schema));
        $sink->write(array_to_rows([['id' => 3, 'name' => 'c']], $schema));
        $sink->close();

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null], ['id' => 3, 'name' => 'c']],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }
}
