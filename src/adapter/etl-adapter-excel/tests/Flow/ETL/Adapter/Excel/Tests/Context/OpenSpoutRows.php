<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use OpenSpout\Common\Entity\Cell;
use OpenSpout\Reader\SheetInterface;

use function array_map;

/**
 * What OpenSpout's own RowIterator reads from a sheet - the oracle XlsxSheetRows is held to.
 */
final class OpenSpoutRows
{
    /**
     * @param SheetInterface<\OpenSpout\Reader\RowIteratorInterface> $sheet
     *
     * @return list<array<int, mixed>>
     */
    public static function of(SheetInterface $sheet): array
    {
        $rows = [];

        foreach ($sheet->getRowIterator() as $row) {
            $rows[] = array_map(static fn(Cell $cell): mixed => $cell->getValue(), $row->cells);
        }

        return $rows;
    }
}
