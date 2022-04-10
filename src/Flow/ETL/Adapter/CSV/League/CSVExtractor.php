<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\League;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;

/**
 * @psalm-immutable
 */
final class CSVExtractor implements Extractor
{
    private ?Reader $reader;

    public function __construct(
        private readonly string $path,
        private readonly int $rowsInBatch,
        private readonly ?int $headerOffset = null,
        private readonly string $operationMode = 'r',
        private readonly string $rowEntryName = 'row',
        private readonly string $delimiter = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\'
    ) {
        $this->reader = null;
    }

    public function extract() : \Generator
    {
        $rows = [];

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array $row
         */
        foreach ($this->reader()->getIterator() as $row) {
            $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row));

            if (\count($rows) >= $this->rowsInBatch) {
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows)) {
            yield new Rows(...$rows);
        }
    }

    /**
     * @psalm-suppress InvalidNullableReturnType
     * @psalm-suppress InaccessibleProperty
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress NullableReturnStatement
     */
    private function reader() : Reader
    {
        if ($this->reader === null) {
            $this->reader = Reader::createFromPath($this->path, $this->operationMode);
            $this->reader->setHeaderOffset($this->headerOffset);
            $this->reader->setDelimiter($this->delimiter);
            $this->reader->setEnclosure($this->enclosure);
            $this->reader->setEscape($this->escape);
        }

        return $this->reader;
    }
}
