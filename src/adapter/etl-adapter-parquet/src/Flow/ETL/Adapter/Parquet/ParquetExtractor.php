<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PartitionFiltering;
use Flow\ETL\Extractor\PartitionsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Partitions;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\Reader;

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
                    $row['_input_file_uri'] = $fileData['uri'];
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $fileData['partitions'], $flowSchema);

                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->close($this->path);

                    return;
                }
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, uri: string, partitions: Partitions}>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->path, $this->partitionFilter()) as $filePath) {
            yield [
                'file' => (new Reader(
                    byteOrder: $this->byteOrder,
                    options: $this->options,
                ))
                    ->readStream($context->streams()->fs()->open($filePath, Mode::READ)->resource()),
                'uri' => $filePath->uri(),
                'partitions' => $filePath->partitions(),
            ];
        }
    }
}
