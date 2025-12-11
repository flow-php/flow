<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{Node, NullTest, NullTestType};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

final readonly class IsNull implements Condition
{
    public function __construct(
        public Expression $expression,
        public bool $negated = false,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $nullTest = $node->getNullTest();

        if ($nullTest === null) {
            throw InvalidAstException::unexpectedNodeType('NullTest', 'unknown');
        }

        $arg = $nullTest->getArg();

        if ($arg === null) {
            throw InvalidAstException::missingRequiredField('arg', 'NullTest');
        }

        $nullTestType = $nullTest->getNulltesttype();

        if ($nullTestType !== NullTestType::IS_NULL && $nullTestType !== NullTestType::IS_NOT_NULL) {
            throw InvalidAstException::invalidFieldValue('nulltesttype', 'NullTest', 'Expected IS_NULL or IS_NOT_NULL');
        }

        $negated = $nullTestType === NullTestType::IS_NOT_NULL;

        return new self(
            ExpressionFactory::fromAst($arg),
            $negated
        );
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function not() : NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other) : OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst() : Node
    {
        $nullTestType = $this->negated ? NullTestType::IS_NOT_NULL : NullTestType::IS_NULL;

        $nullTest = new NullTest([
            'arg' => $this->expression->toAst(),
            'nulltesttype' => $nullTestType,
        ]);

        return new Node(['null_test' => $nullTest]);
    }
}
