<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\{array_to_row, rows};
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\ETL\{Exception\InvalidArgumentException, Extractor, FlowContext};
use Flow\Filesystem\{Path, SourceStream};
use Flow\Parquet\{ByteOrder, Options, ParquetFile, Reader};

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN;

    /**
     * @var array<string>
     */
    private array $columns = [];

    private ?int $offset = null;

    private Options $options;

    private SchemaConverter $schemaConverter;

    /**
     * @param Path $path
     */
    public function __construct(private readonly Path $path)
    {
        $this->resetLimit();
        $this->schemaConverter = new SchemaConverter();
        $this->options = Options::default();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context) as $fileData) {
            $fileRows = $fileData['file']->metadata()->rowsNumber();
            $flowSchema = $this->schemaConverter->fromParquet($fileData['file']->schema());

            if (count($this->columns)) {
                $flowSchema = $flowSchema->keep(...$this->columns);
            }

            if ($fileOffset > $fileRows) {
                $fileData['stream']->close();
                $fileOffset -= $fileRows;

                continue;
            }

            foreach ($fileData['file']->values($this->columns, $this->limit(), $fileOffset) as $row) {
                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $fileData['stream']->path()->uri();
                }

                $signal = yield rows(array_to_row($row, $context->entryFactory(), $fileData['stream']->path()->partitions(), $flowSchema));

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

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

    public function withByteOrder(ByteOrder $byteOrder) : self
    {
        $this->byteOrder = $byteOrder;

        return $this;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns) : self
    {
        $this->columns = $columns;

        return $this;
    }

    public function withOffset(int $offset) : self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withOptions(Options $options) : self
    {
        $this->options = $options;

        return $this;
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
