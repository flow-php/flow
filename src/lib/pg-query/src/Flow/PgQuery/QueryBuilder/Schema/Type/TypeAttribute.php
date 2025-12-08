<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

final readonly class TypeAttribute
{
    public function __construct(
        public string $name,
        public string $type,
        public ?string $collation = null,
    ) {
    }

    public static function of(string $name, string $type) : self
    {
        return new self($name, $type);
    }

    public function collate(string $collation) : self
    {
        return new self($this->name, $this->type, $collation);
    }
}
