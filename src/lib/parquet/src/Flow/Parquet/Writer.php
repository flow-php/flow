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
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;

final class Writer
{
    public const VERSION = 2;

    private int $fileOffset = 0;

    private ?Metadata $metadata = null;

    private ?RowGroupBuilder $rowGroupBuilder = null;

    /**
     * @var null|resource
     */
    private $stream;

    public function __construct(
        private Compressions $compression = Compressions::SNAPPY,
        private Options $options = new Options()
    ) {
        switch ($this->compression) {
            case Compressions::UNCOMPRESSED:
            case Compressions::SNAPPY:
            case Compressions::GZIP:
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

        \fseek($this->stream(), -4, SEEK_END);

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
     * @param iterable<array<string, mixed>> $rows
     */
    public function write(string $path, Schema $schema, iterable $rows) : void
    {
        $this->open($path, $schema);

        $this->writeBatch($rows);

        $this->close();
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function writeBatch(iterable $rows) : void
    {
        foreach ($rows as $row) {
            $this->writeRow($row);
        }
    }

    /**
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
        $this->metadata = (new Metadata($schema, new RowGroups([]), 0, self::VERSION, 'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version']));
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
