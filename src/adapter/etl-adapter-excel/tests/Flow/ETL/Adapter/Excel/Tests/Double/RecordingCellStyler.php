<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Double;

use Flow\ETL\Adapter\Excel\CellStyler;
use Flow\ETL\Schema\Definition;
use OpenSpout\Common\Entity\Style\Style;

use function is_scalar;

final class RecordingCellStyler implements CellStyler
{
    /**
     * @var list<string> "sheet:value@rowNumber" of every first-column cell
     */
    public array $firstColumnCells = [];

    public function style(
        mixed $value,
        Definition $definition,
        int $rowNumber,
        int $columnIndex,
        string $sheetName,
    ): ?Style {
        if ($columnIndex === 0) {
            $this->firstColumnCells[] =
                $sheetName . ':' . (is_scalar($value) ? (string) $value : '') . '@' . $rowNumber;
        }

        return null;
    }
}
