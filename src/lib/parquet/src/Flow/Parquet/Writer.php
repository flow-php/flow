<?php declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroupBuilder;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;

final class Writer
{
    public const VERSION = 2;

    private ?RowGroupBuilder $rowGroupBuilder = null;

    public function __construct(private Options $options = new Options())
    {
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     */
    public function write(string $path, Schema $schema, iterable $rows) : void
    {
        // This will be later replaced with append
        if (\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} already exists");
        }

        $stream = \fopen($path, 'w');

        if ($stream === false) {
            throw new RuntimeException("Can't open {$path} for writing");
        }

        \fwrite($stream, ParquetFile::PARQUET_MAGIC_NUMBER);

        foreach ($rows as $row) {
            $this->rowGroupBuilder($schema)->addRow($row);
        }

        $metadata = (new Metadata($schema, new RowGroups([]), 0, self::VERSION, 'flow-parquet'));

        $rowGroupContainer = $this->rowGroupBuilder($schema)->flush(\strlen(ParquetFile::PARQUET_MAGIC_NUMBER));

        \fwrite($stream, $rowGroupContainer->binaryBuffer);

        $metadata->rowGroups()->add($rowGroupContainer->rowGroup);

        $start = \ftell($stream);
        $metadata->toThrift()->write(new TCompactProtocol(new TPhpFileStream($stream)));
        $end = \ftell($stream);
        $size = $end - $start;
        \fwrite($stream, \pack('l', $size));
        \fwrite($stream, ParquetFile::PARQUET_MAGIC_NUMBER);

        \fseek($stream, -4, SEEK_END);

        \fclose($stream);
    }

    private function rowGroupBuilder(Schema $schema) : RowGroupBuilder
    {
        if ($this->rowGroupBuilder === null) {
            $this->rowGroupBuilder = new RowGroupBuilder($schema, DataConverter::initialize($this->options));
        }

        return $this->rowGroupBuilder;
    }
}
