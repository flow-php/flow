<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

use function extension_loaded;

final readonly class AdaptiveParquetOpenSink implements FileSink, ParquetOpenSink
{
    private ParquetOpenSink $sink;

    /**
     * The engine decides the lane, as for AdaptiveParquetOpenSource: a writer it opens on arrow-ext takes the native
     * columns when flow_php is loaded.
     */
    public function __construct(
        DestinationStream $stream,
        ParquetSchema $schema,
        Compressions $compressions,
        Options $options,
        ?ParquetEngine $engine = null,
    ) {
        $file = ($engine ?? new AdaptiveParquetEngine(ByteOrder::LITTLE_ENDIAN, $options))->openForWrite(
            $stream,
            $schema,
            $compressions,
            $options,
        );

        $this->sink = extension_loaded('flow_php') && $file instanceof RustParquetFileWriter
            ? new RustParquetOpenSink($file)
            : new PhpParquetOpenSink($file, new ParquetEncoder($schema));
    }

    public function close(): void
    {
        $this->sink->close();
    }

    public function write(Rows $rows): void
    {
        $this->sink->write($rows);
    }
}
