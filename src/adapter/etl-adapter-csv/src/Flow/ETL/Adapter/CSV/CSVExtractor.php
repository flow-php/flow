<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PartitionExtractor, PathFiltering, Signal};
use Flow\ETL\Row\Schema;
use Flow\ETL\{Extractor, FlowContext};
use Flow\Filesystem\Path;

final class CSVExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionExtractor
{
    use Limitable;
    use PathFiltering;

    /**
     * @param ?int<1, max> $charactersReadInLine
     */
    public function __construct(
        private readonly Path $path,
        private readonly bool $withHeader = true,
        private readonly bool $emptyToNull = true,
        private readonly ?string $separator = null,
        private readonly ?string $enclosure = null,
        private readonly ?string $escape = null,
        private readonly ?int $charactersReadInLine = null,
        private readonly ?Schema $schema = null
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {

            $option = \Flow\ETL\Adapter\CSV\csv_detect_separator($stream);

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;

            $headers = [];

            foreach ($stream->readLines(length: $this->charactersReadInLine) as $csvLine) {
                if ($this->withHeader && \count($headers) === 0) {
                    /** @var array<string> $headers */
                    $headers = \str_getcsv($csvLine, $separator, $enclosure, $escape);

                    continue;
                }

                /** @var array<mixed> $rowData */
                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);

                if (!\count($headers)) {
                    $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowData) - 1));
                }

                $headers = \array_map(fn (string $header) : string => \trim($header), $headers);
                $headers = \array_map(fn (string $header, int $index) : string => $header !== '' ? $header : 'e' . \str_pad((string) $index, 2, '0', STR_PAD_LEFT), $headers, \array_keys($headers));

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
                    continue;
                }

                $row = \array_combine($headers, $rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $stream->path()->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $stream->path()->partitions(), $this->schema);
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeWriters($this->path);

                    return;
                }

                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);
            }

            $stream->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }
}
