<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetEncoder;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetFileWriter;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetSchemaConformance;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class ParquetFileWriterTest extends FlowTestCase
{
    public function test_written_batches_read_back_equal(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $parquetSchema = (new SchemaConverter())->toParquetSchema($schema);
        $writer = new ParquetFileWriter(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://out.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetEncoder($parquetSchema),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $schema));
        $writer->write(array_to_rows([['id' => 3, 'name' => 'c']], $schema));
        $writer->close();

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null], ['id' => 3, 'name' => 'c']],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_a_column_of_another_stored_type_is_conformed_before_it_is_written(): void
    {
        $filesystem = memory_filesystem();
        $parquetSchema = (new SchemaConverter())->toParquetSchema(schema(int_schema('id', nullable: true)));
        $writer = new ParquetFileWriter(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://out.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetEncoder($parquetSchema),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => '1'], ['id' => '2']], schema(str_schema('id'))));
        $writer->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }
}
