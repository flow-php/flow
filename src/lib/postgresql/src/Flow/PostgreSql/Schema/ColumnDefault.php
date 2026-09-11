<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser\ExpressionParser;
use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

use function array_key_exists;
use function Flow\Types\DSL\type_instance_of;
use function str_replace;

/**
 * A column or domain default modeled as its semantic parts rather than a bare string.
 *
 * PostgreSQL stores a constant default as a (literal, type) pair — `'0'` is kept as
 * `'0'::numeric` on a numeric column and `'0'::double precision` on a double precision column.
 * Comparing only the stripped literal is blind to a stale cast (a default typed against the wrong
 * type, e.g. left behind by `ALTER COLUMN ... TYPE` without a re-issued `SET DEFAULT`). This object
 * keeps the effective cast type so {@see self::equals()} can distinguish that drift, while
 * non-constant defaults (functions, sequences, `now()`) keep comparing by their normalized
 * expression string exactly as before.
 *
 * @import-type ColumnTypeShape from ColumnType
 *
 * @type ColumnDefaultShape = array{literal: string, type: ?ColumnTypeShape, kind: string}
 */
final readonly class ColumnDefault
{
    private function __construct(
        public string $literal,
        public ?ColumnType $effectiveType,
        public DefaultKind $kind,
    ) {}

    /**
     * Single entry point for both introspected (raw pg_get_expr output) and declared
     * (ColumnDefaultFormatter output) defaults.
     *
     * @param ColumnType $columnType the type of the owning column/domain — used as the effective
     *                               type of a constant that carries no explicit cast
     */
    public static function fromExpression(string $expression, ColumnType $columnType): self
    {
        $node = (new ExpressionParser())->parse($expression);

        $typeCast = $node->getTypeCast();

        if ($typeCast !== null) {
            $aConst = $typeCast->getArg()?->getAConst();
            $literal = $aConst !== null ? self::renderConstant($aConst) : null;

            if ($literal !== null) {
                $typeName = $typeCast->getTypeName();

                return new self(
                    $literal,
                    $typeName !== null ? ColumnType::fromAst($typeName) : $columnType,
                    DefaultKind::CONSTANT,
                );
            }
        } else {
            $aConst = $node->getAConst();
            $literal = $aConst !== null ? self::renderConstant($aConst) : null;

            if ($literal !== null) {
                return new self($literal, $columnType, DefaultKind::CONSTANT);
            }
        }

        return new self((new ExpressionParser())->normalize($expression), null, DefaultKind::EXPRESSION);
    }

    /**
     * @param ColumnDefaultShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['literal'],
            array_key_exists('type', $data) && $data['type'] !== null ? ColumnType::fromArray($data['type']) : null,
            DefaultKind::from($data['kind']),
        );
    }

    public static function nullableEquals(?self $a, ?self $b): bool
    {
        if ($a === null || $b === null) {
            return $a === null && $b === null;
        }

        return $a->equals($b);
    }

    /**
     * The expression to feed into a `SET DEFAULT` / `DEFAULT` clause.
     *
     * For a constant this is the bare literal (no cast) so PostgreSQL re-types it against the column,
     * which is exactly what converges a stale cast back to the column's type.
     */
    public function applicableSql(): string
    {
        return $this->literal;
    }

    public function equals(self $other): bool
    {
        if ($this->kind !== $other->kind) {
            return false;
        }

        if ($this->kind === DefaultKind::EXPRESSION) {
            return $this->literal === $other->literal;
        }

        return (
            $this->literal === $other->literal
            && $this->effectiveType !== null
            && $other->effectiveType !== null
            && $this->effectiveType->isSameBaseType($other->effectiveType)
        );
    }

    /**
     * @return ColumnDefaultShape
     */
    public function normalize(): array
    {
        return [
            'literal' => $this->literal,
            'type' => $this->effectiveType?->normalize(),
            'kind' => $this->kind->value,
        ];
    }

    private static function renderConstant(A_Const $aConst): ?string
    {
        $sval = $aConst->getSval();

        if ($sval !== null) {
            return "'" . str_replace("'", "''", $sval->getSval()) . "'";
        }

        if ($aConst->getIval() !== null) {
            return (string) type_instance_of(Integer::class)->assert($aConst->getIval())->getIval();
        }

        $fval = $aConst->getFval();

        if ($fval !== null) {
            return $fval->getFval();
        }

        if ($aConst->getBoolval() !== null) {
            return type_instance_of(Boolean::class)->assert($aConst->getBoolval())->getBoolval() ? 'true' : 'false';
        }

        return null;
    }
}
