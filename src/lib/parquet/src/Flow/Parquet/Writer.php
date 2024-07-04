<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Composer\InstalledVersions;
use Flow\Filesystem\Stream\{NativeLocalDestinationStream};
use Flow\Filesystem\{DestinationStream, Path};
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator;
use Flow\Parquet\ParquetFile\{Compressions, Metadata, RowGroupBuilder, RowGroups, Schema};
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;

final class Writer
{
    private int $fileOffset = 0;

    private ?Metadata $metadata = null;

    private ?RowGroupBuilder $rowGroupBuilder = null;

    private ?DestinationStream $stream = null;

    public function __construct(
        private readonly Compressions $compression = Compressions::SNAPPY,
        private readonly Options $options = new Options(),
    ) {
        switch ($this->compression) {
            case Compressions::UNCOMPRESSED:
            case Compressions::SNAPPY:
            case Compressions::GZIP:
            case Compressions::LZ4:
            case Compressions::LZ4_RAW:
            case Compressions::ZSTD:
                break;

            default:
                throw new InvalidArgumentException("Compression \"{$this->compression->name}\" is not supported yet");
        }
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

        if (!$this->rowGroupBuilder()->isEmpty()) {
            $rowGroupContainer = $this->rowGroupBuilder()->flush($this->fileOffset);
            $this->stream()->append($rowGroupContainer->binaryBuffer);
            $this->metadata()->rowGroups()->add($rowGroupContainer->rowGroup);
            $this->fileOffset += \strlen($rowGroupContainer->binaryBuffer);
        }

        $this->rowGroupBuilder = null;

        $metadataHandle = \fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

        if ($metadataHandle === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        $this->metadata()->toThrift()->write(new TCompactProtocol(new TPhpFileStream($metadataHandle)));
        $metadata = \stream_get_contents($metadataHandle, offset: 0);

        if ($metadata === false) {
            throw new RuntimeException('Cannot read metadata from temporary stream');
        }

        $this->stream()->append($metadata);
        \fclose($metadataHandle);

        $size = \strlen($metadata);

        $this->stream()->append(\pack('l', $size));
        $this->stream()->append(ParquetFile::PARQUET_MAGIC_NUMBER);

        $this->stream()->close();

        $this->stream = null;
        $this->fileOffset = 0;
    }

    public function isOpen() : bool
    {
        return $this->stream !== null;
    }

    public function open(string $path, Schema $schema) : void
    {
        if ($this->isOpen()) {
            throw new RuntimeException('Writer is already open');
        }

        // This will be later replaced with append
        if (\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} already exists");
        }

        $stream = NativeLocalDestinationStream::openBlank(new Path($path));

        $this->stream = $stream;
        $this->stream()->append(ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->fileOffset = \strlen(ParquetFile::PARQUET_MAGIC_NUMBER);

        $this->initMetadata($schema);
        $this->initGroupBuilder($schema);
    }

    /**
     * Opens a writer for an existing stream.
     */
    public function openForStream(DestinationStream $stream, Schema $schema) : void
    {
        $this->stream = $stream;

        $this->stream()->append(ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->fileOffset = \strlen(ParquetFile::PARQUET_MAGIC_NUMBER);

        $this->initMetadata($schema);

        $this->initGroupBuilder($schema);
    }

    /**
     * Create new parquet file, write rows, write metadata and close the file.
     *
     * @param iterable<array<string, mixed>> $rows
     */
    public function write(string $path, Schema $schema, iterable $rows) : void
    {
        $this->open($path, $schema);

        $this->writeBatch($rows);

        $this->close();
    }

    /**
     * Write a batch of rows into a parquet file.
     * Before using this method, you should call open() or openForStream() method to open the writer.
     * Once all rows are written, you should call close() method to close the writer.
     *
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeBatch(iterable $rows) : void
    {
        foreach ($rows as $row) {
            $this->writeRow($row);
        }
    }

    /**
     * Write a single row into a parquet file.
     * Before using this method, you should call open() or openForStream() method to open the writer.
     * Once all rows are written, you should call close() method to close the writer.
     *
     * @param array<string, mixed> $row
     */
    public function writeRow(array $row) : void
    {
        $this->rowGroupBuilder()->addRow($row);
        $interval = (int) $this->options->get(Option::ROW_GROUP_SIZE_CHECK_INTERVAL);

        if (($this->rowGroupBuilder()->statistics()->rowsCount() % $interval === 0) && $this->rowGroupBuilder()->isFull()) {
            $rowGroupContainer = $this->rowGroupBuilder()->flush($this->fileOffset);
            $this->stream()->append($rowGroupContainer->binaryBuffer);
            $this->metadata()->rowGroups()->add($rowGroupContainer->rowGroup);
            $this->fileOffset += \strlen($rowGroupContainer->binaryBuffer);
        }
    }

    /**
     * Create new parquet file directly in stream, write rows, write metadata and close the file.
     *
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeStream(DestinationStream $resource, Schema $schema, iterable $rows) : void
    {
        $this->openForStream($resource, $schema);

        $this->writeBatch($rows);

        $this->close();
    }

    private function initGroupBuilder(Schema $schema) : void
    {
        if ($this->rowGroupBuilder === null) {
            $this->rowGroupBuilder = new RowGroupBuilder(
                $schema,
                $this->compression,
                $this->options,
                DataConverter::initialize($this->options),
                new PageSizeCalculator($this->options)
            );
        } else {
            throw new RuntimeException('RowGroupBuilder is already initialized, please close the writer first before initializing a new RowGroupBuilder');
        }
    }

    private function initMetadata(Schema $schema) : void
    {
        $this->metadata = (new Metadata($schema, new RowGroups([]), 0, $this->options->getInt(Option::WRITER_VERSION), 'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version']));
    }

    private function metadata() : Metadata
    {
        if ($this->metadata === null) {
            throw new RuntimeException('Writer is not open');
        }

        return $this->metadata;
    }

    private function rowGroupBuilder() : RowGroupBuilder
    {
        if ($this->rowGroupBuilder === null) {
            throw new RuntimeException('Writer is not open');
        }

        return $this->rowGroupBuilder;
    }

    private function stream() : DestinationStream
    {
        if ($this->stream === null) {
            throw new RuntimeException('Writer is not open');
        }

        return $this->stream;
    }
}
