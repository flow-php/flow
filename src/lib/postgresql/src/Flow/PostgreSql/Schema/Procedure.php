<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\{column_type_from_string, create};

use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class Procedure
{
    /**
     * @param list<string> $argumentTypes
     */
    public function __construct(
        public string $name,
        public array $argumentTypes = [],
        public string $language = 'sql',
        public ?string $definition = null,
    ) {
    }

    public function toSql() : ?SqlQuery
    {
        if ($this->definition === null) {
            return null;
        }

        $builder = create()->procedure($this->name)->orReplace();

        if ($this->argumentTypes !== []) {
            $args = \array_map(
                static fn (string $type) : FunctionArgument => FunctionArgument::of(column_type_from_string($type)),
                $this->argumentTypes,
            );
            $builder = $builder->arguments(...$args);
        }

        return $builder->language($this->language)->as($this->definition);
    }
}
