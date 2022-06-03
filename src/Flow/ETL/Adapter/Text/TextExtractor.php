<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

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
final class TextExtractor implements Extractor
{
    /**
     * @var null|resource
     * @psalm-allow-private-mutation
     */
    private $resource;

    public function __construct(
        private readonly FileStream $stream,
        private readonly int $rowsInBatch = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     */
    public function extract() : \Generator
    {
        /** @var array<Row> $rows */
        $rows = [];

        $rowData = \fgets($this->stream());

        if ($rowData === false) {
            return;
        }

        while ($rowData !== false) {
            $rows[] = Row::create(new Row\Entry\StringEntry($this->rowEntryName, $rowData));

            if (\count($rows) >= $this->rowsInBatch) {
                yield new Rows(...$rows);

                /** @var array<Row> $rows */
                $rows = [];
            }

            $rowData = \fgets($this->stream());
        }

        if (\count($rows)) {
            yield new Rows(...$rows);
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
