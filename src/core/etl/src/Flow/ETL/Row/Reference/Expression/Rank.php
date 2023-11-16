<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\WindowFunction;

final class Rank implements WindowFunction
{
    private ?Window $window;

    public function __construct()
    {
        $this->window = null;
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
        $rank = 1;

        $orderBy = $this->window()->order();

        if (\count($orderBy) > 1) {
            throw new \RuntimeException('Rank window function supports only one order by column');
        }

        if (\count($orderBy) === 0) {
            throw new \RuntimeException('Rank window function requires to be ordered by one column');
        }

        $value = $row->valueOf($orderBy[0]->name());

        foreach ($partition->sortBy(...$orderBy) as $partitionRow) {

            $partitionValue = $partitionRow->valueOf($orderBy[0]->name());

            if ($value < $partitionValue) {
                $rank++;
            }
        }

        return $rank;
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    public function toString() : string
    {
        return 'rank()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
