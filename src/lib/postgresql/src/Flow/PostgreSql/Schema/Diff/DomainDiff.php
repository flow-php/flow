<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Domain;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\drop;

final readonly class DomainDiff implements Diff
{
    /**
     * @param list<CheckConstraint> $addedCheckConstraints
     * @param list<CheckConstraint> $removedCheckConstraints
     */
    public function __construct(
        public Domain $source,
        public Domain $target,
        public array $addedCheckConstraints = [],
        public array $removedCheckConstraints = [],
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        if ($this->hasBaseTypeChanged()) {
            return [drop()->domain($this->target->name)->cascade(), $this->target->toSql()];
        }

        $sqls = [];

        if ($this->target->nullable !== $this->source->nullable) {
            $sqls[] = $this->target->nullable
                ? alter()->domain($this->target->name)->dropNotNull()
                : alter()->domain($this->target->name)->setNotNull();
        }

        if ($this->target->default !== $this->source->default) {
            $sqls[] = $this->target->default === null
                ? alter()->domain($this->target->name)->dropDefault()
                : alter()
                    ->domain($this->target->name)
                    ->setDefault(ExpressionFactory::fromAst((new ExpressionParser())->parse($this->target->default)));
        }

        foreach ($this->removedCheckConstraints as $cc) {
            if ($cc->name === null) {
                throw new \RuntimeException(\sprintf(
                    'Cannot drop unnamed check constraint on domain "%s". Constraint names are required for reversible migrations.',
                    $this->target->name,
                ));
            }

            $sqls[] = alter()->domain($this->target->name)->dropConstraint($cc->name);
        }

        foreach ($this->addedCheckConstraints as $cc) {
            if ($cc->name === null) {
                throw new \RuntimeException(\sprintf(
                    'Cannot add unnamed check constraint on domain "%s". Constraint names are required for reversible migrations.',
                    $this->target->name,
                ));
            }

            $sqls[] = alter()
                ->domain($this->target->name)
                ->addConstraint($cc->name, ConditionFactory::fromAst((new ExpressionParser())->parse($cc->expression)));
        }

        return $sqls;
    }

    public function hasBaseTypeChanged(): bool
    {
        return !$this->source->baseType->isEqual($this->target->baseType);
    }

    public function hasDefaultChanged(): bool
    {
        return $this->source->default !== $this->target->default;
    }

    public function hasNullableChanged(): bool
    {
        return $this->source->nullable !== $this->target->nullable;
    }
}
