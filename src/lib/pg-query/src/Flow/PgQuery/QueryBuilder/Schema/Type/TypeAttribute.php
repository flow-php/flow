<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\QueryBuilder\Schema\DataType;

final readonly class TypeAttribute
{
    public function __construct(
        public string $name,
        public DataType $type,
        public ?string $collation = null,
    ) {
    }

    public static function of(string $name, DataType $type) : self
    {
        return new self($name, $type);
    }

    public function collate(string $collation) : self
    {
        return new self($this->name, $this->type, $collation);
    }
}
