<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Sheet;

use Flow\ETL\Exception\InvalidArgumentException;
use OpenSpout\Reader\{SheetInterface, SheetIteratorInterface};

final readonly class SheetsManager
{
    public function __construct(private SheetIteratorInterface $sheets)
    {
    }

    public function first() : SheetInterface
    {
        // Reset iterator when the previous search could be applied
        $this->sheets->rewind();

        return $this->sheets->current();
    }

    public function get(string $sheetName) : SheetInterface
    {
        SheetNameAssertion::assert($sheetName);

        // Reset iterator when the previous search could be applied
        $this->sheets->rewind();

        foreach ($this->sheets as $sheet) {
            if ($sheet->getName() === $sheetName) {
                return $sheet;
            }
        }

        throw new InvalidArgumentException("Sheet with name: '{$sheetName}' not found.");
    }
}
