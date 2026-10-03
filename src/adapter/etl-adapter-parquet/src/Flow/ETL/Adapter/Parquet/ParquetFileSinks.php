<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Loader\File\FileSinks;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

final class ParquetFileSinks implements FileSinks
{
    private ?ParquetSchemaConformance $conformance = null;

    private ?ParquetSchema $parquetSchema = null;

    /**
     * @param null|Schema $declared the schema every file is written under, partition columns removed; null to take
     *                              the first written batch's schema, made nullable
     */
    public function __construct(
        private readonly ?Schema $declared,
        private readonly SchemaConverter $converter,
        private readonly Compressions $compressions,
        private readonly Options $options,
        private readonly ?ParquetEngine $engine,
    ) {}

    /**
     * $rows conformed to the run's Parquet schema - a column of another stored type cast to the writer's. The first
     * call fixes that schema: the declared one, or $rows' made nullable.
     */
    public function conform(Rows $rows, Backend $backend): Rows
    {
        $this->parquetSchema ??= $this->converter->toParquet($this->declared ?? $rows->schema()->makeNullable());

        return ($this->conformance ??= new ParquetSchemaConformance($this->parquetSchema))->conform($rows, $backend);
    }

    public function open(DestinationStream $stream, Backend $backend): FileSink
    {
        return new ParquetFileSink($this, $stream, $backend);
    }

    /**
     * The writer of one stream under the run's Parquet schema.
     *
     * @throws RuntimeException before the first conform() fixed the schema
     */
    public function writer(DestinationStream $stream): AdaptiveParquetOpenSink
    {
        return new AdaptiveParquetOpenSink(
            $stream,
            $this->parquetSchema ?? throw new RuntimeException('The Parquet schema is fixed by the first conform()'),
            $this->compressions,
            $this->options,
            $this->engine,
        );
    }
}
