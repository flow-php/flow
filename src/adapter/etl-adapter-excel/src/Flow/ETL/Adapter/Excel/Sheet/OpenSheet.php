<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Sheet;

use Flow\ETL\Adapter\Excel\ExcelEncoder;
use Generator;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

/**
 * @import-type SheetCellValue from SheetCells
 */
final readonly class OpenSheet
{
    /**
     * @param Generator<int, array<int, SheetCellValue>> $cells
     */
    public function __construct(
        private XlsxReader|OdsReader $reader,
        public Generator $cells,
        public ExcelEncoder $encoder,
    ) {}

    public function close(): void
    {
        $this->reader->close();
    }
}
