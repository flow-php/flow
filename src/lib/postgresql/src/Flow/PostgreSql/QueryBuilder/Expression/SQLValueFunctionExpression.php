<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SQLValueFunction;
use Flow\PostgreSql\Protobuf\AST\SQLValueFunctionOp;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;

use function in_array;

/**
 * Represents SQL standard value functions like CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME.
 *
 * These are special SQL keywords that return values based on the current transaction time.
 * Unlike regular functions, they don't use parentheses and are represented in PostgreSQL's
 * AST as SQLValueFunction nodes rather than FuncCall nodes.
 */
final readonly class SQLValueFunctionExpression implements Expression
{
    private function __construct(
        private int $op,
    ) {}

    public static function currentCatalog(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_CATALOG);
    }

    public static function currentDate(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_DATE);
    }

    public static function currentRole(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_ROLE);
    }

    public static function currentSchema(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA);
    }

    public static function currentTime(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_TIME);
    }

    public static function currentTimestamp(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP);
    }

    public static function currentUser(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_CURRENT_USER);
    }

    public static function fromAst(Node $node): static
    {
        $sqlValueFunction = $node->getSqlValueFunction();

        if ($sqlValueFunction === null) {
            throw InvalidAstException::unexpectedNodeType('SQLValueFunction', 'unknown');
        }

        $op = $sqlValueFunction->getOp();

        $supported = [
            SQLValueFunctionOp::SVFOP_CURRENT_DATE,
            SQLValueFunctionOp::SVFOP_CURRENT_TIME,
            SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP,
            SQLValueFunctionOp::SVFOP_CURRENT_ROLE,
            SQLValueFunctionOp::SVFOP_CURRENT_USER,
            SQLValueFunctionOp::SVFOP_CURRENT_CATALOG,
            SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA,
            SQLValueFunctionOp::SVFOP_LOCALTIME,
            SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP,
            SQLValueFunctionOp::SVFOP_SESSION_USER,
            SQLValueFunctionOp::SVFOP_USER,
        ];

        if (!in_array($op, $supported, true)) {
            throw UnsupportedNodeException::forNodeType('SQLValueFunction op=' . $op);
        }

        return new self($op);
    }

    public static function localTime(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_LOCALTIME);
    }

    public static function localTimestamp(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP);
    }

    public static function sessionUser(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_SESSION_USER);
    }

    public static function user(): self
    {
        return new self(SQLValueFunctionOp::SVFOP_USER);
    }

    public function as(string $alias): AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function toAst(): Node
    {
        $sqlValueFunction = new SQLValueFunction();
        $sqlValueFunction->setOp($this->op);
        $sqlValueFunction->setTypmod(-1);

        $node = new Node();
        $node->setSqlValueFunction($sqlValueFunction);

        return $node;
    }
}
