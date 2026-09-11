<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\PeerComparator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;
use RuntimeException as BaseRuntimeException;

use function count;
use function Flow\Types\DSL\type_integer;

final class DenseRank implements PartitionRanking, WindowFunction
{
    use ResolvesFromChildren;

    public function __construct(
        private readonly ?Window $window = null,
    ) {}

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    public function apply(WindowContext $window): mixed
    {
        return $this->rankPartition($window->partition())[$window->index()];
    }

    /**
     * @return list<int>
     */
    public function rankPartition(Rows $partition): array
    {
        $orderBy = $this->window()->order();

        if (count($orderBy) === 0) {
            throw new BaseRuntimeException('Dens Rank window function requires to be ordered by one column');
        }

        $comparator = new PeerComparator($orderBy);
        $ranks = [];
        $rank = 1;
        $previous = null;

        foreach ($partition as $row) {
            if ($previous !== null && !$comparator->arePeers($previous, $row, $partition->schema())) {
                $rank++;
            }

            $ranks[] = $rank;
            $previous = $row;
        }

        return $ranks;
    }

    public function over(Window $window): static
    {
        return new self($window);
    }

    /**
     * NOT NULL - every row of a partition has a dense rank
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
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
