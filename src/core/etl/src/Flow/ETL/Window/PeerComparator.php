<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Schema;

/**
 * Two rows are peers when they carry equal values in every ORDER BY column. Compared with
 * TypedValueComparator rather than ===, so two DateTimeImmutable instances of the same instant are peers.
 */
final readonly class PeerComparator
{
    /**
     * @param array<Reference> $orderBy
     */
    public function __construct(
        private array $orderBy,
    ) {}

    public function arePeers(Row $left, Row $right, Schema $schema): bool
    {
        $comparator = new TypedValueComparator();

        foreach ($this->orderBy as $ref) {
            if (!$comparator->equals($schema->get($ref)->type(), $left->get($ref), $right->get($ref))) {
                return false;
            }
        }

        return true;
    }
}
