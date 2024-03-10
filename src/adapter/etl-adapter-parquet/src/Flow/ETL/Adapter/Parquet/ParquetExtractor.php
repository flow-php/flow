<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PartitionFiltering, PartitionsExtractor, Signal};
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\{Extractor, FlowContext};
use Flow\Parquet\{ByteOrder, Options, ParquetFile, Reader};

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionsExtractor
{
    use Limitable;
    use PartitionFiltering;

    private SchemaConverter $schemaConverter;

    /**
     * @param Path $path
     * @param array<string> $columns
     */
    public function __construct(
        private readonly Path $path,
        private readonly Options $options,
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private readonly array $columns = [],
        private readonly ?int $offset = null
    ) {
        $this->resetLimit();
        $this->schemaConverter = new SchemaConverter();

        if ($this->path->isPattern() && $this->offset !== null) {
            throw new InvalidArgumentException('Offset can be used only with single file path, not with pattern');
        }
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($this->readers($context) as $fileData) {
            $flowSchema = $this->schemaConverter->fromParquet($fileData['file']->schema());

            foreach ($fileData['file']->values($this->columns, $this->limit(), $this->offset) as $row) {
                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $fileData['stream']->path()->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $fileData['stream']->path()->partitions(), $flowSchema);

                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeWriters($this->path);

                    return;
                }
            }

            $fileData['stream']->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, stream: FileStream}>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->scan($this->path, $this->partitionFilter()) as $stream) {
            yield [
                'file' => (new Reader(
                    byteOrder: $this->byteOrder,
                    options: $this->options,
                ))
                    ->readStream($stream->resource()),
                'stream' => $stream,
            ];
        }
    }
}
