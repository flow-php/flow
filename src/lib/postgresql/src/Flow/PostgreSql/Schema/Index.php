<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

final readonly class Index
{
    /**
     * @param non-empty-list<string> $columns
     */
    public function __construct(
        public string $name,
        public array $columns,
        public bool $unique = false,
        public IndexMethod $method = IndexMethod::BTREE,
        public bool $primary = false,
        public ?string $predicate = null,
    ) {
    }

    public function isEqual(self $other) : bool
    {
        return $this->name === $other->name && $this->isEqualStructure($other);
    }

    public function isEqualStructure(self $other) : bool
    {
        return $this->columns === $other->columns
            && $this->unique === $other->unique
            && $this->method === $other->method
            && $this->primary === $other->primary
            && $this->predicate === $other->predicate;
    }
}
