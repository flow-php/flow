<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{FuncCall, Node, PBString};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

/**
 * Represents a function call: func(args).
 */
final readonly class FunctionCall implements Expression
{
    /**
     * @param non-empty-list<string> $funcName Function name parts (e.g., ['pg_catalog', 'count'] or ['my_func'])
     * @param list<Expression> $args Function arguments
     */
    public function __construct(
        private array $funcName,
        private array $args = [],
    ) {
        if ($this->funcName === []) {
            throw InvalidExpressionException::emptyArray('Function name');
        }
    }

    public static function fromAst(Node $node) : static
    {
        $funcCall = $node->getFuncCall();

        if ($funcCall === null) {
            throw InvalidAstException::unexpectedNodeType('FuncCall', 'unknown');
        }

        $funcNameNodes = $funcCall->getFuncname();

        if ($funcNameNodes === null || \count($funcNameNodes) === 0) {
            throw InvalidAstException::missingRequiredField('funcname', 'FuncCall');
        }

        $funcName = [];

        foreach ($funcNameNodes as $nameNode) {
            $stringNode = $nameNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('funcname', 'FuncCall', 'expected String node');
            }

            $funcName[] = $stringNode->getSval();
        }

        if ($funcName === []) {
            throw InvalidAstException::invalidFieldValue('funcname', 'FuncCall', 'cannot be empty');
        }

        $args = [];
        $argsNodes = $funcCall->getArgs();

        if ($argsNodes !== null) {
            foreach ($argsNodes as $argNode) {
                $args[] = ExpressionFactory::fromAst($argNode);
            }
        }

        return new self($funcName, $args);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    /**
     * @return list<Expression>
     */
    public function getArgs() : array
    {
        return $this->args;
    }

    /**
     * @return non-empty-list<string>
     */
    public function getFuncName() : array
    {
        return $this->funcName;
    }

    public function toAst() : Node
    {
        $funcCall = new FuncCall();
        $funcNameNodes = [];

        foreach ($this->funcName as $namePart) {
            $stringNode = new PBString();
            $stringNode->setSval($namePart);

            $nameNode = new Node();
            $nameNode->setString($stringNode);

            $funcNameNodes[] = $nameNode;
        }

        $funcCall->setFuncname($funcNameNodes);

        $argNodes = [];

        foreach ($this->args as $arg) {
            $argNodes[] = $arg->toAst();
        }

        $funcCall->setArgs($argNodes);

        $node = new Node();
        $node->setFuncCall($funcCall);

        return $node;
    }

    public function withArgs(Expression ...$args) : self
    {
        return new self($this->funcName, \array_values($args));
    }
}
