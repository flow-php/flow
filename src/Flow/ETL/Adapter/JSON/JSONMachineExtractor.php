<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\Items;

/**
 * @psalm-immutable
 */
final class JSONMachineExtractor implements Extractor
{
    private Items $jsonItems;

    private int $rowsInBatch;

    private string $rowEntryName;

    public function __construct(Items $jsonItems, int $rowsInBatch, string $rowEntryName = 'row')
    {
        $this->jsonItems = $jsonItems;
        $this->rowsInBatch = $rowsInBatch;
        $this->rowEntryName = $rowEntryName;
    }

    public function extract() : \Generator
    {
        $rows = new Rows();

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array|object $row
         */
        foreach ($this->jsonItems->getIterator() as $row) {
            $rows = $rows->add(Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, (array) $row)));

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
