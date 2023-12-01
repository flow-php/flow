<?php declare(strict_types=1);

namespace Flow\Parquet;

use Composer\InstalledVersions;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroupBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Thrift\FileMetaData;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;

final class Writer
{
    private int $fileOffset = 0;

    private ?Metadata $metadata = null;

    private ?RowGroupBuilder $rowGroupBuilder = null;

    /**
     * @var null|resource
     */
    private $stream;

    public function __construct(
        private Compressions $compression = Compressions::SNAPPY,
        private Options $options = new Options(),
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN
    ) {
        switch ($this->compression) {
            case Compressions::UNCOMPRESSED:
            case Compressions::SNAPPY:
            case Compressions::GZIP:
            case Compressions::BROTLI:
            case Compressions::LZ4:
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

    /**
     * Reopen existing parquet file, read metadata and append new rows.
     * Once all rows are appended, the file is automatically closed.
     *
     * @param iterable<array<string, mixed>> $rows
     */
    public function append(string $path, iterable $rows) : void
    {
        $this->reopen($path);

        $this->writeBatch($rows);

        $this->close();
    }

    public function close() : void
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Writer is not open');
        }

        if (!$this->rowGroupBuilder()->isEmpty()) {
            $rowGroupContainer = $this->rowGroupBuilder()->flush($this->fileOffset);
            \fwrite($this->stream(), $rowGroupContainer->binaryBuffer);
            $this->metadata()->rowGroups()->add($rowGroupContainer->rowGroup);
            $this->fileOffset += \strlen($rowGroupContainer->binaryBuffer);
        }

        $this->rowGroupBuilder = null;

        $start = \ftell($this->stream());
        $this->metadata()->toThrift()->write(new TCompactProtocol(new TPhpFileStream($this->stream())));
        $end = \ftell($this->stream());
        $size = $end - $start;
        \fwrite($this->stream(), \pack('l', $size));
        \fwrite($this->stream(), ParquetFile::PARQUET_MAGIC_NUMBER);

        /** @psalm-suppress InvalidPassByReference */
        \fclose($this->stream());

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

        $stream = \fopen($path, 'wb');

        if ($stream === false) {
            throw new RuntimeException("Can't open {$path} for writing");
        }

        $this->stream = $stream;
        \fwrite($this->stream(), ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->fileOffset = \strlen(ParquetFile::PARQUET_MAGIC_NUMBER);

        $this->initMetadata($schema);
        $this->initGroupBuilder($schema);
    }

    /**
     * Opens a writer for an existing stream.
     *
     * @param resource $resource
     */
    public function openForStream($resource, Schema $schema) : void
    {
        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('Given argument is not a valid resource');
        }

        $streamMetadata = \stream_get_meta_data($resource);

        if (!$streamMetadata['seekable']) {
            throw new InvalidArgumentException('Given stream is not seekable');
        }

        if (!\str_starts_with($streamMetadata['mode'], 'wb')) {
            throw new InvalidArgumentException('Given stream is not opened in write mode, expected wb, got: ' . $streamMetadata['mode']);
        }

        $this->stream = $resource;

        \fseek($this->stream(), 0);
        \fwrite($this->stream(), ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->fileOffset = \strlen(ParquetFile::PARQUET_MAGIC_NUMBER);

        $this->initMetadata($schema);

        $this->initGroupBuilder($schema);
    }

    /**
     * Reopen existing Parquet file for appending new rows.
     * This method will read the metadata from the end of the file and truncate the file.
     */
    public function reopen(string $path) : void
    {
        if ($this->isOpen()) {
            throw new RuntimeException('Writer is already open');
        }

        if (!\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} don't exists");
        }

        $stream = \fopen($path, 'ab+');

        if ($stream === false) {
            throw new RuntimeException("Can't open {$path} for writing");
        }

        $this->stream = $stream;

        \fseek($this->stream(), -4, SEEK_END);

        if (\fread($this->stream(), 4) !== ParquetFile::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        \fseek($this->stream(), -8, SEEK_END);

        /**
         * @phpstan-ignore-next-line
         */
        $metadataLength = \unpack($this->byteOrder->value, \fread($this->stream(), 4))[1];
        \fseek($this->stream(), -($metadataLength + 8), SEEK_END);

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new TCompactProtocol(new TPhpFileStream($this->stream())));

        $this->metadata = Metadata::fromThrift($thriftMetadata);

        $this->initGroupBuilder($this->metadata()->schema());

        \fseek($this->stream(), -($metadataLength + 8), SEEK_END);

        $fileSizeWithoutMetadata = \ftell($this->stream());

        if ($fileSizeWithoutMetadata === false || $fileSizeWithoutMetadata <= 0) {
            throw new RuntimeException('File is empty');
        }

        // Truncate previous metadata
        \ftruncate($this->stream(), $fileSizeWithoutMetadata);

        $this->fileOffset = $fileSizeWithoutMetadata;
    }

    /**
     * Opens a writer for an existing stream.
     *
     * @param resource $resource
     */
    public function reopenForStream($resource) : void
    {
        if ($this->isOpen()) {
            throw new RuntimeException('Writer is already open');
        }

        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('Given argument is not a valid resource');
        }

        $streamMetadata = \stream_get_meta_data($resource);

        if (!$streamMetadata['seekable']) {
            throw new InvalidArgumentException('Given stream is not seekable');
        }

        \fseek($resource, 0);

        $this->stream = $resource;

        \fseek($this->stream(), -4, SEEK_END);

        if (\fread($this->stream(), 4) !== ParquetFile::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        \fseek($this->stream(), -8, SEEK_END);

        /**
         * @phpstan-ignore-next-line
         */
        $metadataLength = \unpack($this->byteOrder->value, \fread($this->stream(), 4))[1];
        \fseek($this->stream(), -($metadataLength + 8), SEEK_END);

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new TCompactProtocol(new TPhpFileStream($this->stream())));

        $this->metadata = Metadata::fromThrift($thriftMetadata);

        $this->initGroupBuilder($this->metadata()->schema());

        \fseek($this->stream(), -($metadataLength + 8), SEEK_END);

        $fileSizeWithoutMetadata = \ftell($this->stream());

        if ($fileSizeWithoutMetadata === false || $fileSizeWithoutMetadata <= 0) {
            throw new RuntimeException('File is empty');
        }

        // Truncate previous metadata
        \ftruncate($this->stream(), $fileSizeWithoutMetadata);

        $this->fileOffset = $fileSizeWithoutMetadata;
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
            \fwrite($this->stream(), $rowGroupContainer->binaryBuffer);
            $this->metadata()->rowGroups()->add($rowGroupContainer->rowGroup);
            $this->fileOffset += \strlen($rowGroupContainer->binaryBuffer);
        }
    }

    /**
     * Create new parquet file directly in stream, write rows, write metadata and close the file.
     *
     * @param resource $resource
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeStream($resource, Schema $schema, iterable $rows) : void
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

    /**
     * @return resource
     */
    private function stream()
    {
        if ($this->stream === null) {
            throw new RuntimeException('Writer is not open');
        }

        return $this->stream;
    }
}
