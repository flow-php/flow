<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use function sort;

/**
 * @type UniqueConstraintShape = array{columns: non-empty-list<string>, name?: ?string, nulls_not_distinct?: bool}
 */
final readonly class UniqueConstraint
{
    /**
     * @param non-empty-list<string> $columns
     */
    public function __construct(
        public array $columns,
        public ?string $name = null,
        public bool $nullsNotDistinct = false,
    ) {}

    /**
     * @param UniqueConstraintShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            columns: $data['columns'],
            name: $data['name'] ?? null,
            nullsNotDistinct: $data['nulls_not_distinct'] ?? false,
        );
    }

    public function isEqual(self $other): bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other): bool
    {
        $aCols = $this->columns;
        $bCols = $other->columns;
        sort($aCols);
        sort($bCols);

        return $aCols === $bCols && $this->nullsNotDistinct === $other->nullsNotDistinct;
    }

    /**
     * @return UniqueConstraintShape
     */
    public function normalize(): array
    {
        return [
            'columns' => $this->columns,
            'name' => $this->name,
            'nulls_not_distinct' => $this->nullsNotDistinct,
        ];
    }
}
