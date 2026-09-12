<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;

use function file_exists;
use function Flow\Filesystem\DSL\path;

final class Writer
{
    private ?ParquetFileWriter $file = null;

    public function __construct(
        private readonly Compressions $compression = Compressions::SNAPPY,
        private readonly Options $options = new Options(),
        private readonly ParquetEngine $engine = new AdaptiveParquetEngine(),
    ) {
        match ($this->compression) {
            Compressions::UNCOMPRESSED,
            Compressions::SNAPPY,
            Compressions::BROTLI,
            Compressions::GZIP,
            Compressions::LZ4,
            Compressions::LZ4_RAW,
            Compressions::ZSTD,
                => null,
            default => throw new InvalidArgumentException(
                "Compression \"{$this->compression->name}\" is not supported yet",
            ),
        };
    }

    public static function arrow(
        Compressions $compression = Compressions::SNAPPY,
        Options $options = new Options(),
    ): self {
        return new self($compression, $options, new ArrowParquetEngine());
    }

    public static function php(Compressions $compression = Compressions::SNAPPY, Options $options = new Options()): self
    {
        return new self($compression, $options, new PhpParquetEngine());
    }

    public function __destruct()
    {
        if ($this->isOpen()) {
            $this->close();
        }
    }

    public function close(): void
    {
        $file = $this->file ?? throw new RuntimeException('Writer is not open');

        try {
            $file->close();
        } finally {
            $this->file = null;
        }
    }

    public function isOpen(): bool
    {
        return $this->file !== null;
    }

    public function open(string $path, Schema $schema): void
    {
        if ($this->isOpen()) {
            throw new RuntimeException('Writer is already open');
        }

        if (file_exists($path)) {
            throw new InvalidArgumentException("File {$path} already exists");
        }

        $this->file = $this->engine->openForWrite(
            NativeLocalDestinationStream::openBlank(path($path)),
            $schema,
            $this->compression,
            $this->options,
        );
    }

    public function openForStream(DestinationStream $stream, Schema $schema): void
    {
        $this->file = $this->engine->openForWrite($stream, $schema, $this->compression, $this->options);
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function write(string $path, Schema $schema, iterable $rows): void
    {
        if (file_exists($path)) {
            throw new InvalidArgumentException("File {$path} already exists");
        }

        $this->engine->writeRows(
            NativeLocalDestinationStream::openBlank(path($path)),
            $schema,
            $this->compression,
            $this->options,
            $rows,
        );
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeBatch(iterable $rows): void
    {
        ($this->file ?? throw new RuntimeException('Writer is not open'))->writeBatch($rows);
    }

    /**
     * @param array<string, mixed> $row
     */
    public function writeRow(array $row): void
    {
        ($this->file ?? throw new RuntimeException('Writer is not open'))->writeRow($row);
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeStream(DestinationStream $stream, Schema $schema, iterable $rows): void
    {
        $this->engine->writeRows($stream, $schema, $this->compression, $this->options, $rows);
    }
}
