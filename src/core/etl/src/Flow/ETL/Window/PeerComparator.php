<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

/**
 * Two rows are peers when they carry equal values in every ORDER BY column. Compared with
 * Entry::isEqual() rather than ===, so two DateTimeImmutable instances of the same instant are peers.
 */
final readonly class PeerComparator
{
    /**
     * @param array<Reference> $orderBy
     */
    public function __construct(
        private array $orderBy,
    ) {}

    public function arePeers(Row $left, Row $right): bool
    {
        foreach ($this->orderBy as $ref) {
            if (!$left->get($ref)->isEqual($right->get($ref))) {
                return false;
            }
        }

        return true;
    }
}
