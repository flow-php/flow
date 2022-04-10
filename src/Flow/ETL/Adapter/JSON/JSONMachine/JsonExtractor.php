<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\Items;

/**
 * @psalm-immutable
 */
final class JsonExtractor implements Extractor
{
    private readonly Items $jsonItems;

    public function __construct(
        string $filePath,
        private readonly
        int $rowsInBatch,
        private readonly string $rowEntryName = 'row'
    ) {
        $this->jsonItems = Items::fromFile($filePath);
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
