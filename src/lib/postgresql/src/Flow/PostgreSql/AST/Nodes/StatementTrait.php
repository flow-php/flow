<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;

trait StatementTrait
{
    /**
     * @template S of Statement<mixed>
     *
     * @param class-string<S> $statementClass
     *
     * @throws InvalidStatementException
     *
     * @return S
     */
    public function assert(string $statementClass) : Statement
    {
        if (!$this instanceof $statementClass) {
            throw new InvalidStatementException(
                \sprintf('Expected statement of type %s, got %s', $statementClass, static::class)
            );
        }

        return $this;
    }
}
