<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use WeakMap;

use function array_reverse;

final readonly class PeerFrame implements WindowFrame
{
    /**
     * @var WeakMap<Rows, list<int>> per partition: the last row of each row's peer group, derived once
     */
    private WeakMap $ends;

    /**
     * @param array<Reference> $orderBy
     */
    public function __construct(
        private array $orderBy,
    ) {
        $this->ends = new WeakMap();
    }

    public function bounds(int $index, Rows $partition): array
    {
        if ($index > ($partition->count() - 1)) {
            return [1, 0];
        }

        return [0, $this->ends($partition)[$index]];
    }

    /**
     * @return list<int> for every row the index of the last row of its peer group
     */
    public function ends(Rows $partition): array
    {
        if (!$this->ends->offsetExists($partition)) {
            $peers = (new PeerComparator($this->orderBy))->peersOfPrevious($partition);
            $ends = [];
            $end = $partition->count() - 1;

            // walking back, a row that is not a peer of the one before it closes the previous group
            for ($i = $end; $i >= 0; $i--) {
                $ends[] = $end;

                if ($i > 0 && !$peers[$i]) {
                    $end = $i - 1;
                }
            }

            $this->ends[$partition] = array_reverse($ends);
        }

        return $this->ends[$partition];
    }
}
