<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\Reader;

final class ParquetExtractor implements Extractor, Extractor\FileExtractor
{
    /**
     * @param Path $path
     * @param array<string> $columns
     */
    public function __construct(
        private readonly Path $path,
        private readonly Options $options,
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private readonly array $columns = []
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($this->readers($context) as $fileData) {
            foreach ($fileData['file']->values($this->columns) as $row) {
                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $fileData['uri'];
                }

                yield array_to_rows($row, $context->entryFactory());
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, uri: string}>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            yield [
                'file' => (new Reader(
                    byteOrder: $this->byteOrder,
                    options: $this->options,
                ))
                    ->readStream($context->streams()->fs()->open($filePath, Mode::READ)->resource()),
                'uri' => $filePath->uri(),
            ];
        }
    }
}
