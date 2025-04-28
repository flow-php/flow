<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

final readonly class SheetAddress
{
    private ?string $endCell;

    private string $startCell;

    public function __construct(
        string $startCell = 'A1',
        ?string $endCell = null,
        private ?string $sheetName = null,
    ) {
        $this->startCell = \strtoupper($startCell);
        $this->endCell = $endCell ? \strtoupper($endCell) : null;
    }

    public static function calculate(int $columns, int $rows, ?self $startAddress = null) : self
    {
        $startAddress ??= new self('A1');
        $startColumn = $startAddress->getStartCellColumn();
        $startRow = $startAddress->getStartCellRow();

        $startColumnNumber = 0;

        for ($i = 0, $length = \strlen($startColumn); $i < $length; $i++) {
            $startColumnNumber = $startColumnNumber * 26 + (ord($startColumn[$i]) - 64);
        }

        $endColumnNumber = $startColumnNumber + $columns - 1;
        $endColumn = '';

        while ($endColumnNumber > 0) {
            $remainder = ($endColumnNumber - 1) % 26;
            $endColumn = chr(65 + $remainder) . $endColumn;
            $endColumnNumber = floor(($endColumnNumber - $remainder) / 26);
        }

        return new self(
            $startColumn . $startRow,
            $endColumn . ($startRow + $rows - 1),
            $startAddress->getSheetName()
        );
    }

    public function getEndCell() : ?string
    {
        return $this->endCell;
    }

    public function getEndCellColumn() : string
    {
        return (string) preg_replace('/[0-9]/', '', (string) $this->endCell);
    }

    public function getEndCellRow() : int
    {
        return (int) preg_replace('/[A-Z]/', '', (string) $this->endCell);
    }

    public function getSheetName() : ?string
    {
        return $this->sheetName;
    }

    public function getStartCell() : string
    {
        return $this->startCell;
    }

    public function getStartCellColumn() : string
    {
        return (string) preg_replace('/[0-9]/', '', $this->startCell);
    }

    public function getStartCellRow() : int
    {
        return (int) preg_replace('/[A-Z]/', '', $this->startCell);
    }

    public function toString() : string
    {
        return ($this->sheetName ? ("'" . $this->sheetName . "'!") : '') . $this->startCell . ($this->endCell ? ':' . $this->endCell : '');
    }
}
