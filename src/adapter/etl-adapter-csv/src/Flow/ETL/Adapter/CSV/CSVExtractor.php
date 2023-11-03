<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Extractor\Limitable;
use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;

final class CSVExtractor implements Extractor, Extractor\FileExtractor
{
    use Limitable;

    /**
     * @param int<0, max> $charactersReadInLine
     */
    public function __construct(
        private readonly Path $path,
        private readonly bool $withHeader = true,
        private readonly bool $emptyToNull = true,
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\',
        private readonly int $charactersReadInLine = 1000
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $path) {
            $stream = $context->streams()->fs()->open($path, Mode::READ);

            $rowsData = [];
            $headers = [];

            if ($this->withHeader && \count($headers) === 0) {
                /** @var array<string> $headers */
                $headers = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);
            }

            /** @var array<mixed> $rowPlainData */
            $rowPlainData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);

            if (!\count($headers)) {
                $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowPlainData) - 1));
            }

            while (\is_array($rowPlainData)) {
                if (\count($headers) > \count($rowPlainData)) {
                    \array_push(
                        $rowPlainData,
                        ...\array_map(
                            fn (int $i) => ($this->emptyToNull ? null : ''),
                            \range(1, \count($headers) - \count($rowPlainData))
                        )
                    );
                }

                if (\count($rowData) > \count($headers)) {
                    $rowData = \array_slice($rowData, 0, \count($headers));
                }

                if ($this->emptyToNull) {
                    foreach ($rowPlainData as $i => $data) {
                        if ($data === '') {
                            $rowPlainData[$i] = null;
                        }
                    }
                }

                if (\count($headers) !== \count($rowPlainData)) {
                    $rowPlainData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);

                    continue;
                }

                $rowData = \array_combine($headers, $rowPlainData);

                if ($shouldPutInputIntoRows) {
                    $rowData['_input_file_uri'] = $stream->path()->uri();
                }

                yield array_to_rows($row, $context->entryFactory());

                $rowPlainData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);
            }
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }
}
