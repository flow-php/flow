<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\{column_type_from_string, create};

use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class Func
{
    /**
     * @param list<string> $argumentTypes
     */
    public function __construct(
        public string $name,
        public string $returnType,
        public array $argumentTypes = [],
        public string $language = 'sql',
        public ?string $definition = null,
        public bool $isStrict = false,
        public ?FunctionVolatility $volatility = null,
    ) {
    }

    public function toSql() : ?SqlQuery
    {
        if ($this->definition === null) {
            return null;
        }

        $builder = create()->function($this->name)->orReplace();

        if ($this->argumentTypes !== []) {
            $args = \array_map(
                static fn (string $type) : FunctionArgument => FunctionArgument::of(column_type_from_string($type)),
                $this->argumentTypes,
            );
            $builder = $builder->arguments(...$args);
        }

        $builder = $builder->returns(column_type_from_string($this->returnType))
            ->language($this->language);

        if ($this->isStrict) {
            $builder = $builder->strict();
        }

        if ($this->volatility !== null) {
            $builder = match ($this->volatility) {
                FunctionVolatility::IMMUTABLE => $builder->immutable(),
                FunctionVolatility::STABLE => $builder->stable(),
                FunctionVolatility::VOLATILE => $builder->volatile(),
            };
        }

        return $builder->as($this->definition);
    }
}
