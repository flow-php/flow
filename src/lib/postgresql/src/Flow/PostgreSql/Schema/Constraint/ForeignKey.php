<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Constraint;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;

final readonly class ForeignKey
{
    /**
     * @param non-empty-list<string> $columns
     * @param non-empty-list<string> $referenceColumns
     */
    public function __construct(
        public ?string $name,
        public array $columns,
        public string $referenceSchema,
        public string $referenceTable,
        public array $referenceColumns,
        public ReferentialAction $onUpdate = ReferentialAction::NO_ACTION,
        public ReferentialAction $onDelete = ReferentialAction::NO_ACTION,
        public bool $deferrable = false,
        public bool $initiallyDeferred = false,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->columns === $other->columns
            && $this->referenceSchema === $other->referenceSchema
            && $this->referenceTable === $other->referenceTable
            && $this->referenceColumns === $other->referenceColumns
            && $this->onUpdate === $other->onUpdate
            && $this->onDelete === $other->onDelete
            && $this->deferrable === $other->deferrable
            && $this->initiallyDeferred === $other->initiallyDeferred;
    }
}
