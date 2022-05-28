<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\Mode;

/**
 * @psalm-immutable
 */
final class CSVExtractor implements Extractor
{
    /**
     * @var null|resource
     * @psalm-allow-private-mutation
     */
    private $resource;

    public function __construct(
        private readonly FileStream $stream,
        private readonly int $rowsInBatch = 1000,
        private readonly bool $withHeader = true,
        private readonly bool $emptyToNull = true,
        private readonly string $rowEntryName = 'row',
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\'
    ) {
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     */
    public function extract() : \Generator
    {
        /** @var array<Row> $rows */
        $rows = [];
        $headers = [];

        if ($this->withHeader && \count($headers) === 0) {
            /** @var array<string> $headers */
            $headers = \fgetcsv($this->stream(), 1000, $this->separator, $this->enclosure, $this->escape);
        }

        /** @var array<mixed> $rowData */
        $rowData = \fgetcsv($this->stream(), 1000, $this->separator, $this->enclosure, $this->escape);

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
                /** @phpstan-ignore-next-line  */
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

            $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, \array_combine($headers, $rowData)));

            if (\count($rows) >= $this->rowsInBatch) {
                yield new Rows(...$rows);

                /** @var array<Row> $rows */
                $rows = [];
            }

            $rowData = \fgetcsv($this->stream(), 1000, $this->separator, $this->enclosure, $this->escape);
        }

        if (\count($rows)) {
            yield new Rows(...$rows);
        }

        if (\is_resource($this->resource)) {
            /** @psalm-suppress InvalidPropertyAssignmentValue */
            \fclose($this->resource);
            $this->resource = null;
        }
    }

    /**
     * @throws RuntimeException
     * @psalm-suppress InvalidNullableReturnType
     * @psalm-suppress ImpureMethodCall
     *
     * @return resource
     */
    private function stream()
    {
        if ($this->resource === null) {
            $this->resource = Handler::file()->open($this->stream, Mode::READ);
        }

        /**
         * @psalm-suppress NullableReturnStatement
         */
        return $this->resource;
    }
}
