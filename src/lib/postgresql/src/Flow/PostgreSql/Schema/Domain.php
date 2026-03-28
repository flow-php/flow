<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\create;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;

final readonly class Domain
{
    /**
     * @param list<CheckConstraint> $checkConstraints
     */
    public function __construct(
        public string $name,
        public ColumnType $baseType,
        public bool $nullable = true,
        public ?string $default = null,
        public array $checkConstraints = [],
    ) {
    }

    public function toSql() : SqlQuery
    {
        $builder = create()->domain($this->name)->as($this->baseType);

        if (!$this->nullable) {
            $builder = $builder->notNull();
        }

        if ($this->default !== null) {
            $builder = $builder->default(ExpressionFactory::fromAst((new ExpressionParser(new Parser()))->parse($this->default)));
        }

        foreach ($this->checkConstraints as $cc) {
            if ($cc->name !== null) {
                $builder = $builder->constraint($cc->name);
            }
            $builder = $builder->check(ConditionFactory::fromAst((new ExpressionParser(new Parser()))->parse($cc->expression)));
        }

        return $builder;
    }
}
