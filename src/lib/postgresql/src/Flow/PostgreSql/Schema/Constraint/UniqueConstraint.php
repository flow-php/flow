<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

final readonly class UniqueConstraint
{
    /**
     * @param non-empty-list<string> $columns
     */
    public function __construct(
        public array $columns,
        public ?string $name = null,
        public bool $nullsNotDistinct = false,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        $aCols = $this->columns;
        $bCols = $other->columns;
        \sort($aCols);
        \sort($bCols);

        return $aCols === $bCols && $this->nullsNotDistinct === $other->nullsNotDistinct;
    }
}
