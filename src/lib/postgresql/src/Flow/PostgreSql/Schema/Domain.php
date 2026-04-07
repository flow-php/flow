<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\create;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;

/**
 * @phpstan-import-type ColumnTypeShape from ColumnType
 * @phpstan-import-type CheckConstraintShape from CheckConstraint
 *
 * @phpstan-type DomainShape = array{name: string, base_type: ColumnTypeShape, nullable: bool, default: ?string, check_constraints: list<CheckConstraintShape>}
 */
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

    /**
     * @param DomainShape $data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            name: $data['name'],
            baseType: ColumnType::fromArray($data['base_type']),
            nullable: $data['nullable'] ?? true,
            default: $data['default'] ?? null,
            checkConstraints: \array_map(
                static fn (array $cc) : CheckConstraint => CheckConstraint::fromArray($cc),
                $data['check_constraints'] ?? [],
            ),
        );
    }

    /**
     * @return DomainShape
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'base_type' => $this->baseType->normalize(),
            'nullable' => $this->nullable,
            'default' => $this->default,
            'check_constraints' => \array_map(
                static fn (CheckConstraint $cc) : array => $cc->normalize(),
                $this->checkConstraints,
            ),
        ];
    }

    public function toSql() : Sql
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
