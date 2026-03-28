<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\create;

use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class Extension
{
    public function __construct(
        public string $name,
        public ?string $version = null,
    ) {
    }

    public function toSql() : SqlQuery
    {
        $builder = create()->extension($this->name);

        if ($this->version !== null) {
            $builder = $builder->version($this->version);
        }

        return $builder;
    }
}
