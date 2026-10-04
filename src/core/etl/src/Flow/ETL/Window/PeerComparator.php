<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function is_float;
use function is_nan;

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

    /**
     * NaN is equal to itself here: the sort order puts it after every value.
     *
     * @param Type<mixed> $type
     */
    public function equal(Type $type, mixed $left, mixed $right): bool
    {
        if (is_float($left) && is_float($right) && is_nan($left) && is_nan($right)) {
            return true;
        }

        return (new TypedValueComparator())->equals($type, $left, $right);
    }

    /**
     * @return list<bool> per row: a peer of the row before it (row 0 never is)
     */
    public function peersOfPrevious(Rows $partition): array
    {
        $count = $partition->count();
        $peers = [];

        for ($i = 0; $i < $count; $i++) {
            $peers[] = $i > 0;
        }

        $comparable = new ComparableValues();

        foreach ($this->orderBy as $ref) {
            $type = $partition->schema()->get($ref)->type();
            $column = $partition->column($ref->base());

            if ($comparable->equalByPhysical($type)) {
                // === on physicals is value equality; NaN is the one physical that is not === to itself
                $physicals = $comparable->equality($column);

                for ($i = 1; $i < $count; $i++) {
                    // @mago-ignore analysis:mixed-assignment
                    $previous = $physicals[$i - 1];
                    // @mago-ignore analysis:mixed-assignment
                    $current = $physicals[$i];

                    if (
                        $peers[$i]
                        && $previous !== $current
                        && !(is_float($previous) && is_float($current) && is_nan($previous) && is_nan($current))
                    ) {
                        $peers[$i] = false;
                    }
                }

                continue;
            }

            $values = $column->values();

            for ($i = 1; $i < $count; $i++) {
                if ($peers[$i] && !$this->equal($type, $values[$i - 1], $values[$i])) {
                    $peers[$i] = false;
                }
            }
        }

        return $peers;
    }
}
