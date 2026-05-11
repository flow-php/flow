<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\IdentityGeneration;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\column;

final readonly class ColumnDiff implements Diff
{
    public function __construct(
        public string $qualifiedTableName,
        public Column $source,
        public Column $target,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $sqls = [];
        $columnName = $this->source->name;

        if ($this->source->name !== $this->target->name) {
            $sqls[] = alter()->table($this->qualifiedTableName)->renameColumn($this->source->name, $this->target->name);
            $columnName = $this->target->name;
        }

        // PostgreSQL cannot ALTER generated columns or identity — must drop and re-add
        if (
            $this->target->isGenerated !== $this->source->isGenerated
            || $this->target->generationExpression !== $this->source->generationExpression
            || $this->target->isIdentity !== $this->source->isIdentity
            || $this->target->identityGeneration !== $this->source->identityGeneration
        ) {
            $sqls[] = alter()->table($this->qualifiedTableName)->dropColumn($columnName);

            $colDef = column($this->target->name, $this->target->type);

            if (!$this->target->nullable) {
                $colDef = $colDef->notNull();
            }

            if ($this->target->default !== null) {
                $colDef = $colDef->defaultRaw(ExpressionFactory::fromAst((new ExpressionParser())->parse($this->target->default)));
            }

            if ($this->target->isGenerated && $this->target->generationExpression !== null) {
                $colDef = $colDef->generatedAs(ExpressionFactory::fromAst((new ExpressionParser())->parse($this->target->generationExpression)));
            }

            if ($this->target->isIdentity) {
                $colDef = $colDef->identity($this->target->identityGeneration ?? IdentityGeneration::ALWAYS);
            }

            $sqls[] = alter()->table($this->qualifiedTableName)->addColumn($colDef);

            return $sqls;
        }

        if (!$this->target->type->isEqual($this->source->type)) {
            $sqls[] = alter()->table($this->qualifiedTableName)->alterColumnType($columnName, $this->target->type);
        }

        if ($this->target->nullable !== $this->source->nullable) {
            $sqls[] = $this->target->nullable
                ? alter()->table($this->qualifiedTableName)->alterColumnDropNotNull($columnName)
                : alter()->table($this->qualifiedTableName)->alterColumnSetNotNull($columnName);
        }

        if ($this->target->default !== $this->source->default) {
            $sqls[] = $this->target->default === null
                ? alter()->table($this->qualifiedTableName)->alterColumnDropDefault($columnName)
                : alter()
                    ->table($this->qualifiedTableName)
                    ->alterColumnSetDefault(
                        $columnName,
                        ExpressionFactory::fromAst((new ExpressionParser())->parse($this->target->default)),
                    );
        }

        return $sqls;
    }

    public function hasDefaultChanged(): bool
    {
        return $this->source->default !== $this->target->default;
    }

    public function hasGenerationChanged(): bool
    {
        return (
            $this->source->isGenerated !== $this->target->isGenerated
            || $this->source->generationExpression !== $this->target->generationExpression
        );
    }

    public function hasIdentityChanged(): bool
    {
        return (
            $this->source->isIdentity !== $this->target->isIdentity
            || $this->source->identityGeneration !== $this->target->identityGeneration
        );
    }

    public function hasNameChanged(): bool
    {
        return $this->source->name !== $this->target->name;
    }

    public function hasNullableChanged(): bool
    {
        return $this->source->nullable !== $this->target->nullable;
    }

    public function hasTypeChanged(): bool
    {
        return !$this->source->type->isEqual($this->target->type);
    }
}
