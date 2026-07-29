<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\PeerComparator;
use Flow\ETL\Window\WindowContext;
use RuntimeException as BaseRuntimeException;

use function count;

final class Rank implements PartitionRanking, WindowFunction
{
    private ?Window $window;

    public function __construct()
    {
        $this->window = null;
    }

    public function apply(WindowContext $window): mixed
    {
        return $this->rankPartition($window->partition())[$window->index()];
    }

    public function rankPartition(Rows $partition): array
    {
        $orderBy = $this->window()->order();

        if (count($orderBy) === 0) {
            throw new BaseRuntimeException('Rank window function requires to be ordered by one column');
        }

        $comparator = new PeerComparator($orderBy);
        $ranks = [];
        $rank = 1;
        $index = 0;
        $previous = null;

        foreach ($partition as $row) {
            if ($previous !== null && !$comparator->arePeers($previous, $row)) {
                $rank = $index + 1;
            }

            $ranks[] = $rank;
            $previous = $row;
            $index++;
        }

        return $ranks;
    }

    public function over(Window $window): static
    {
        $this->window = $window;

        return $this;
    }

    public function toString(): string
    {
        return 'rank()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
