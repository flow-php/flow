<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;

use Flow\ETL\Row\Factory\NativeEntryFactory;

final class CSVExtractor implements Extractor
{
    /**
     * @param int<0, max> $charactersReadInLine
     */
    public function __construct(
        private readonly Path $uri,
        private readonly int $rowsInBatch = 1000,
        private readonly bool $withHeader = true,
        private readonly bool $emptyToNull = true,
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\',
        private readonly int $charactersReadInLine = 1000,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->uri, $context->partitionFilter()) as $path) {
            $stream = $context->streams()->fs()->open($path, Mode::READ);

            /** @var array<Row> $rows */
            $rows = [];
            $headers = [];

            if ($this->withHeader && \count($headers) === 0) {
                /** @var array<string> $headers */
                $headers = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);
            }

            /** @var array<mixed> $rowData */
            $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);

            if (!\count($headers)) {
                $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowData) - 1));
            }

            while (\is_array($rowData)) {
                if (\count($headers) > \count($rowData)) {
                    \array_push(
                        $rowData,
                        ...\array_map(
                            /** @psalm-suppress UnusedClosureParam */
                            fn (int $i) => ($this->emptyToNull ? null : ''),
                            \range(1, \count($headers) - \count($rowData))
                        )
                    );
                }

                if (\count($rowData) > \count($headers)) {
                    /** @phpstan-ignore-next-line */
                    $rowData = \array_chunk($rowData, \count($headers));
                }

                if ($this->emptyToNull) {
                    /** @psalm-suppress MixedAssignment */
                    foreach ($rowData as $i => $data) {
                        if ($data === '') {
                            $rowData[$i] = null;
                        }
                    }
                }

                if (\count($headers) !== \count($rowData)) {
                    $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);

                    continue;
                }

                if ($context->config->shouldPutInputIntoRows()) {
                    $rows[] = \array_merge(\array_combine($headers, $rowData), ['_input_file_uri' => $stream->path()->uri()]);
                } else {
                    $rows[] = \array_combine($headers, $rowData);
                }

                if (\count($rows) >= $this->rowsInBatch) {
                    yield array_to_rows($rows, $this->entryFactory);

                    /** @var array<Row> $rows */
                    $rows = [];
                }

                $rowData = \fgetcsv($stream->resource(), $this->charactersReadInLine, $this->separator, $this->enclosure, $this->escape);
            }

            if ([] !== $rows) {
                yield array_to_rows($rows, $this->entryFactory);
            }

            if ($stream->isOpen()) {
                $stream->close();
            }
        }
    }
}
