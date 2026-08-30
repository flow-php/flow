<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row\Reference;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Rows;

final readonly class PeerFrame implements WindowFrame
{
    /**
     * @param array<Reference> $orderBy
     */
    public function __construct(
        private array $orderBy,
    ) {}

    public function bounds(int $index, Rows $partition): array
    {
        $lastIndex = $partition->count() - 1;

        if ($index > $lastIndex) {
            return [1, 0];
        }

        $row = $partition[$index];
        $end = $index;
        $schema = $partition->schema();
        $comparator = new TypedValueComparator();

        while ($end < $lastIndex) {
            $next = $partition[$end + 1];

            foreach ($this->orderBy as $ref) {
                if (!$comparator->equals($schema->get($ref)->type(), $row->get($ref), $next->get($ref))) {
                    return [0, $end];
                }
            }

            $end++;
        }

        return [0, $end];
    }
}
