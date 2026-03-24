<?php

declare(strict_types=1);

namespace Flow\Parquet;

use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Parquet\Engine\{AdaptiveParquetEngine, ArrowParquetEngine, PhpParquetEngine};
use Flow\Parquet\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Parquet\ParquetFile\{Compressions, Schema};

final class Writer
{
    private bool $isOpen = false;

    public function __construct(
        private readonly Compressions $compression = Compressions::SNAPPY,
        private readonly Options $options = new Options(),
        private readonly ParquetEngine $engine = new AdaptiveParquetEngine(),
    ) {
        switch ($this->compression) {
            case Compressions::UNCOMPRESSED:
            case Compressions::SNAPPY:
            case Compressions::BROTLI:
            case Compressions::GZIP:
            case Compressions::LZ4:
            case Compressions::LZ4_RAW:
            case Compressions::ZSTD:
                break;

            default:
                throw new InvalidArgumentException("Compression \"{$this->compression->name}\" is not supported yet");
        }
    }

    public static function arrow(Compressions $compression = Compressions::SNAPPY, Options $options = new Options()) : self
    {
        return new self($compression, $options, new ArrowParquetEngine());
    }

    public static function php(Compressions $compression = Compressions::SNAPPY, Options $options = new Options()) : self
    {
        return new self($compression, $options, new PhpParquetEngine());
    }

    public function __destruct()
    {
        if ($this->isOpen()) {
            $this->close();
        }
    }

    public function close() : void
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Writer is not open');
        }

        $this->engine->closeWrite();
        $this->isOpen = false;
    }

    public function isOpen() : bool
    {
        return $this->isOpen;
    }

    public function open(string $path, Schema $schema) : void
    {
        if ($this->isOpen()) {
            throw new RuntimeException('Writer is already open');
        }

        if (\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} already exists");
        }

        $this->engine->openForWrite(
            NativeLocalDestinationStream::openBlank(path($path)),
            $schema,
            $this->compression,
            $this->options,
        );
        $this->isOpen = true;
    }

    public function openForStream(DestinationStream $stream, Schema $schema) : void
    {
        $this->engine->openForWrite($stream, $schema, $this->compression, $this->options);
        $this->isOpen = true;
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function write(string $path, Schema $schema, iterable $rows) : void
    {
        if (\file_exists($path)) {
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
    public function writeBatch(iterable $rows) : void
    {
        $this->engine->writeBatch($rows);
    }

    /**
     * @param array<string, mixed> $row
     */
    public function writeRow(array $row) : void
    {
        $this->engine->writeRow($row);
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeStream(DestinationStream $stream, Schema $schema, iterable $rows) : void
    {
        $this->engine->writeRows($stream, $schema, $this->compression, $this->options, $rows);
    }
}
