<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use Flow\ETL\Adapter\Excel\Sheet\SheetCells;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

use function iterator_to_array;

final readonly class OpenSheetCells
{
    public function __construct(
        private XlsxReader|OdsReader $reader,
        private SheetCells $cells,
    ) {}

    /**
     * @return list<array<int, mixed>>
     */
    public function readAll(): array
    {
        $rows = iterator_to_array($this->cells->rows(), false);

        $this->reader->close();

        return $rows;
    }
}
