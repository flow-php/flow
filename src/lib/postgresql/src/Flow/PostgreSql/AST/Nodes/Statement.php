<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;

/**
 * @template-covariant T
 */
interface Statement
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
    public function assert(string $statementClass) : self;

    /**
     * @return T
     */
    public function raw();
}
