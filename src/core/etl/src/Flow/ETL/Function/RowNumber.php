<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class RowNumber implements WindowFunction
{
    private ?Window $window;

    public function __construct()
    {
        $this->window = null;
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
        $number = 1;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            if ($partitionRow->isEqual($row)) {
                return $number;
            }

            $number++;
        }

        return null;
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    public function toString() : string
    {
        return 'row_number()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
