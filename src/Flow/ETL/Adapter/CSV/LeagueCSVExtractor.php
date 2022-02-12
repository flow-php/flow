<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;

/**
 * @deprecated
 * @psalm-immutable
 */
final class LeagueCSVExtractor implements Extractor
{
    private Reader $reader;

    private int $rowsInBatch;

    private string $rowEntryName;

    public function __construct(Reader $reader, int $rowsInBatch, string $rowEntryName = 'row')
    {
        $this->reader = $reader;
        $this->rowsInBatch = $rowsInBatch;
        $this->rowEntryName = $rowEntryName;
    }

    public function extract() : \Generator
    {
        $rows = [];

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array $row
         */
        foreach ($this->reader->getIterator() as $row) {
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
}
