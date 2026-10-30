<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\IO\Loader\File\FileWriterFactory;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\RustParquetFileWriter as ArrowRustParquetFileWriter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

final class ParquetFileWriterFactory implements FileWriterFactory
{
    private ?ParquetSchemaConformance $conformance = null;

    private ?ParquetSchema $parquetSchema = null;

    /**
     * @param null|Schema $declared the schema every file is written under, partition columns removed; null to take
     *                              the first opened stream's schema, made nullable
     */
    public function __construct(
        private readonly ?Schema $declared,
        private readonly SchemaConverter $converter,
        private readonly Compressions $compressions,
        private readonly Options $options,
        private readonly ?ParquetEngine $engine,
        private readonly ?FlowPhpExtension $extension = null,
    ) {}

    /**
     * The writer of one stream under the run's Parquet schema, which the first call fixes: the declared one, or
     * $schema made nullable.
     */
    public function writer(DestinationStream $stream, Schema $schema, Backend $backend): FileWriter
    {
        $parquetSchema =
            $this->parquetSchema ??= $this->converter->toParquetSchema($this->declared ?? $schema->makeNullable());
        $conformance = $this->conformance ??= new ParquetSchemaConformance($parquetSchema);
        // detected before the engine opens a writer, so a skewed flow_php is refused with nothing opened
        $native = ($this->extension ?? FlowPhpExtension::detect())->available();
        // the engine decides the lane: a writer it opens on arrow-ext takes the native columns when flow_php is loaded
        $file = ($this->engine ?? new AdaptiveParquetEngine(ByteOrder::LITTLE_ENDIAN, $this->options))->openForWrite(
            $stream,
            $parquetSchema,
            $this->compressions,
            $this->options,
        );

        return $native && $file instanceof ArrowRustParquetFileWriter
            ? new RustParquetFileWriter($file, $conformance, $backend)
            : new ParquetFileWriter($file, new ParquetEncoder($parquetSchema), $conformance, $backend);
    }
}
