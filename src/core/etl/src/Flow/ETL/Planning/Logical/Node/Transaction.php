<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Transaction as FlowTransaction;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

use function array_values;

/**
 * A ROOT grouping write roots that commit together; never on a spine.
 */
final readonly class Transaction implements Node
{
    /**
     * @var list<Write>
     */
    private array $writes;

    public function __construct(
        public FlowTransaction $transaction,
        Write ...$writes,
    ) {
        if ($writes === []) {
            throw new InvalidArgumentException('At least one write must be provided');
        }

        $this->writes = array_values($writes);
    }

    /**
     * @return list<Write> the sibling ROOTS this transaction commits together - never a row input
     */
    public function children(): array
    {
        return $this->writes;
    }

    public function withChildren(array $children): self
    {
        $writes = [];

        foreach ($children as $child) {
            $writes[] = $child instanceof Write
                ? $child
                : throw InvalidLogicException::writeRootRewritten($child::class);
        }

        return $writes === $this->writes ? $this : new self($this->transaction, ...$writes);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
