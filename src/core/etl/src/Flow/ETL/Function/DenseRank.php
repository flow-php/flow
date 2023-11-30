<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class DenseRank implements WindowFunction
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
            throw new \RuntimeException('Dens Rank window function supports only one order by column');
        }

        if (\count($orderBy) === 0) {
            throw new \RuntimeException('Dens Rank window function requires to be ordered by one column');
        }

        $value = $row->valueOf($orderBy[0]->name());

        $countedValues = [];

        foreach ($partition->sortBy(...$orderBy) as $partitionRow) {

            $partitionValue = $partitionRow->valueOf($orderBy[0]->name());

            if ($value < $partitionValue) {
                if (!\in_array($partitionValue, $countedValues, true)) {
                    $rank++;
                    $countedValues[] = $partitionValue;
                }
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
        return 'dens_rank()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
