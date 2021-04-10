<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\JsonMachine;

/**
 * @psalm-immutable
 */
final class JSONMachineExtractor implements Extractor
{
    private JsonMachine $reader;

    private int $rowsInBatch;

    private string $rowEntryName;

    public function __construct(JsonMachine $reader, int $rowsInBatch, string $rowEntryName = 'row')
    {
        $this->reader = $reader;
        $this->rowsInBatch = $rowsInBatch;
        $this->rowEntryName = $rowEntryName;
    }

    public function extract() : \Generator
    {
        $rows = new Rows();

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array $row
         */
        foreach ($this->reader->getIterator() as $row) {
            $rows = $rows->add(Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row)));

            if ($rows->count() >= $this->rowsInBatch) {
                yield $rows;

                $rows = new Rows();
            }
        }

        if ($rows->count()) {
            yield $rows;
        }
    }
}
