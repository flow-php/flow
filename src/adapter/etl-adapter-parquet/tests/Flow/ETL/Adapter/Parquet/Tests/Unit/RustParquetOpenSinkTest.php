<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\RustParquetOpenSink;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingAppendDestinationStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter as ArrowSchemaConverter;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

#[RequiresPhpExtension('flow_php')]
#[RequiresPhpExtension('arrow')]
final class RustParquetOpenSinkTest extends FlowTestCase
{
    public function test_a_batch_of_php_columns_reads_back_equal(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('name', nullable: true), float_schema('amount'));
        $sink = ParquetFilesContext::nativeSink($filesystem, 'memory://out.parquet', $schema);

        $sink->write(array_to_rows(
            [['id' => 1, 'name' => 'a', 'amount' => 0.14], ['id' => 2, 'name' => null, 'amount' => 1.5]],
            $schema,
            new PhpBackend(),
        ));
        $sink->close();

        static::assertSame(
            [['id' => 1, 'name' => 'a', 'amount' => 0.14], ['id' => 2, 'name' => null, 'amount' => 1.5]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_a_string_that_is_not_valid_utf8_is_refused_naming_column_and_row(): void
    {
        $schema = schema(str_schema('name'));
        $sink = ParquetFilesContext::nativeSink(memory_filesystem(), 'memory://out.parquet', $schema);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Parquet column "name" row 1 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8',
        );

        $sink->write(array_to_rows([['name' => 'ok'], ['name' => "\xff\xfe"]], $schema, new PhpBackend()));
    }

    public function test_an_exception_append_throws_surfaces_as_itself(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $sink = new RustParquetOpenSink(
            new RustParquetFileWriter(
                new FailingAppendDestinationStream($filesystem->writeTo(path('memory://out.parquet'))),
                ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
                Compressions::SNAPPY,
                OptionsConverter::toExtension(Options::default()),
                Options::default()->getInt(Option::ARROW_WRITE_BATCH_SIZE),
            ),
        );
        $sink->write(array_to_rows([['id' => 1]], $schema));

        $this->expectException(FilesystemRuntimeException::class);
        $this->expectExceptionMessage('Appending to "memory://out.parquet" failed');

        $sink->close();
    }
}
