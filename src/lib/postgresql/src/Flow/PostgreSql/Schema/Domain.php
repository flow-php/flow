<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;

use function array_key_exists;
use function array_map;
use function Flow\PostgreSql\DSL\create;

/**
 * @import-type ColumnTypeShape from ColumnType
 * @import-type ColumnDefaultShape from ColumnDefault
 * @import-type CheckConstraintShape from CheckConstraint
 *
 * @type DomainShape = array{name: string, base_type: ColumnTypeShape, nullable: bool, default: ?ColumnDefaultShape, check_constraints: list<CheckConstraintShape>}
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
        public ?ColumnDefault $default = null,
        public array $checkConstraints = [],
    ) {}

    /**
     * @param list<CheckConstraint> $checkConstraints
     */
    public static function create(
        string $name,
        ColumnType $baseType,
        bool $nullable = true,
        bool|float|int|string|Expression|null $default = null,
        array $checkConstraints = [],
    ): self {
        $formattedDefault = (new ColumnDefaultFormatter())->format($default);

        return new self(
            $name,
            $baseType,
            $nullable,
            $formattedDefault === null ? null : ColumnDefault::fromExpression($formattedDefault, $baseType),
            $checkConstraints,
        );
    }

    /**
     * @param DomainShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            baseType: ColumnType::fromArray($data['base_type']),
            nullable: $data['nullable'],
            default: array_key_exists('default', $data) && $data['default'] !== null
                ? ColumnDefault::fromArray($data['default'])
                : null,
            checkConstraints: array_map(static fn(array $cc): CheckConstraint => CheckConstraint::fromArray(
                $cc,
            ), $data['check_constraints']),
        );
    }

    /**
     * @return DomainShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'base_type' => $this->baseType->normalize(),
            'nullable' => $this->nullable,
            'default' => $this->default?->normalize(),
            'check_constraints' => array_map(
                static fn(CheckConstraint $cc): array => $cc->normalize(),
                $this->checkConstraints,
            ),
        ];
    }

    public function toSql(): Sql
    {
        $builder = create()->domain($this->name)->as($this->baseType);

        if (!$this->nullable) {
            $builder = $builder->notNull();
        }

        if ($this->default !== null) {
            $builder = $builder->default(ExpressionFactory::fromAst(
                (new ExpressionParser())->parse($this->default->applicableSql()),
            ));
        }

        foreach ($this->checkConstraints as $cc) {
            if ($cc->name !== null) {
                $builder = $builder->constraint($cc->name);
            }
            $builder = $builder->check(ConditionFactory::fromAst((new ExpressionParser())->parse($cc->expression)));
        }

        return $builder;
    }
}
