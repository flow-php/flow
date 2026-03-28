<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

final readonly class TypeAttribute
{
    public function __construct(
        public string $name,
        public ColumnType $type,
        public ?string $collation = null,
    ) {
    }

    public static function of(string $name, ColumnType $type) : self
    {
        return new self($name, $type);
    }

    public function collate(string $collation) : self
    {
        return new self($this->name, $this->type, $collation);
    }
}
