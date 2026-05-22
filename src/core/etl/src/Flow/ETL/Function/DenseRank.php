<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateInterval;
use DateTimeInterface;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use RuntimeException as BaseRuntimeException;

use function count;
use function in_array;
use function is_array;
use function is_numeric;
use function is_string;

final class DenseRank implements WindowFunction
{
    private ?Window $window;

    public function __construct()
    {
        $this->window = null;
    }

    public function apply(Row $row, Rows $partition, FlowContext $context): mixed
    {
        $rank = 1;

        $orderBy = $this->window()->order();

        if (count($orderBy) > 1) {
            throw new BaseRuntimeException('Dens Rank window function supports only one order by column');
        }

        if (count($orderBy) === 0) {
            throw new BaseRuntimeException('Dens Rank window function requires to be ordered by one column');
        }

        $value = $row->valueOf($orderBy[0]->name());

        $countedValues = [];

        foreach ($partition->sortBy(...$orderBy) as $partitionRow) {
            $partitionValue = $partitionRow->valueOf($orderBy[0]->name());

            $isLess = false;

            if (is_numeric($value) && is_numeric($partitionValue)) {
                $isLess = (float) $value < (float) $partitionValue;
            } elseif (is_string($value) && is_string($partitionValue)) {
                $isLess = $value < $partitionValue;
            } elseif ($value instanceof DateTimeInterface && $partitionValue instanceof DateTimeInterface) {
                $isLess = $value < $partitionValue;
            } elseif ($value instanceof DateInterval && $partitionValue instanceof DateInterval) {
                $isLess = $value < $partitionValue;
            } elseif (is_array($value) && is_array($partitionValue)) {
                $isLess = $value < $partitionValue;
            }

            if ($isLess && !in_array($partitionValue, $countedValues, true)) {
                $rank++;
                $countedValues[] = $partitionValue;
            }
        }

        return $rank;
    }

    public function over(Window $window): WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    public function toString(): string
    {
        return 'dens_rank()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
