<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;
use Throwable;

final readonly class ParquetSourceFileOpener
{
    /**
     * @param list<string> $columns the columns to read, every one when empty
     */
    public function __construct(
        private Filesystem $filesystem,
        private ParquetEngine $engine,
        private Options $options,
        private SchemaConverter $schemaConverter,
        private array $columns,
    ) {}

    /**
     * @throws Throwable the stream is closed when the engine refuses it
     */
    public function open(SourceFile $source): ParquetSourceFile
    {
        $stream = $this->filesystem->readFrom($source->path);

        try {
            $reader = $this->engine->openForRead($stream);
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }

        return new ParquetSourceFile(
            new ParquetFile($stream, $this->options, $reader),
            $source,
            $this->schemaConverter,
            $this->columns,
        );
    }
}
