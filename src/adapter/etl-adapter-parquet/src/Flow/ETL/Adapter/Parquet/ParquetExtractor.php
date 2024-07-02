<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PartitionExtractor, PathFiltering, Signal};
use Flow\ETL\{Extractor, FlowContext};
use Flow\Filesystem\{Path, SourceStream};
use Flow\Parquet\{ByteOrder, Options, ParquetFile, Reader};

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionExtractor
{
    use Limitable;
    use PathFiltering;

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
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context) as $fileData) {
            $fileRows = $fileData['file']->metadata()->rowsNumber();
            $flowSchema = $this->schemaConverter->fromParquet($fileData['file']->schema());

            if ($fileOffset > $fileRows) {
                $fileData['stream']->close();
                $fileOffset -= $fileRows;

                continue;
            }

            foreach ($fileData['file']->values($this->columns, $this->limit(), $fileOffset) as $row) {
                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $fileData['stream']->path()->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $fileData['stream']->path()->partitions(), $flowSchema);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeWriters($this->path);

                    return;
                }
            }

            $fileOffset = max($fileOffset - $fileRows, 0);
            $fileData['stream']->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, stream: SourceStream}>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            yield [
                'file' => (new Reader(byteOrder: $this->byteOrder, options: $this->options))
                    ->readStream($stream),
                'stream' => $stream,
            ];
        }
    }
}
