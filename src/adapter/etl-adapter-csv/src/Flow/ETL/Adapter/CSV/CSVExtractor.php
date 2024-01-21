<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\array_to_rows;
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
use Flow\ETL\Row\Schema;

final class CSVExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionsExtractor
{
    use Limitable;
    use PartitionFiltering;

    /**
     * @param int<0, max> $charactersReadInLine
     */
    public function __construct(
        private readonly Path $path,
        private readonly bool $withHeader = true,
        private readonly bool $emptyToNull = true,
        private readonly string|null $separator = null,
        private readonly string|null $enclosure = null,
        private readonly string|null $escape = null,
        private readonly int $charactersReadInLine = 1000,
        private readonly Schema|null $schema = null
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $this->partitionFilter()) as $path) {
            $stream = $context->streams()->fs()->open($path, Mode::READ);

            $option = \Flow\ETL\Adapter\CSV\csv_detect_separator($stream->resource());

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;

            $headers = [];

            if ($this->withHeader && \count($headers) === 0) {
                /** @var array<string> $headers */
                $headers = \fgetcsv($stream->resource(), $this->charactersReadInLine, $separator, $enclosure, $escape);
            }

            /** @var array<mixed> $rowData */
            $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $separator, $enclosure, $escape);

            if (!\count($headers)) {
                $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowData) - 1));
            }

            while (\is_array($rowData)) {
                if (\count($headers) > \count($rowData)) {
                    \array_push(
                        $rowData,
                        ...\array_map(
                            fn (int $i) => ($this->emptyToNull ? null : ''),
                            \range(1, \count($headers) - \count($rowData))
                        )
                    );
                }

                if (\count($rowData) > \count($headers)) {
                    $rowData = \array_slice($rowData, 0, \count($headers));
                }

                if ($this->emptyToNull) {
                    foreach ($rowData as $i => $data) {
                        if ($data === '') {
                            $rowData[$i] = null;
                        }
                    }
                }

                if (\count($headers) !== \count($rowData)) {
                    $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $separator, $enclosure, $escape);

                    continue;
                }

                $row = \array_combine($headers, $rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $stream->path()->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $path->partitions(), $this->schema);
                $this->countRow();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->close($this->path);

                    return;
                }

                $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $separator, $enclosure, $escape);
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }
}
