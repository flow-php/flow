<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

/**
 * @template-covariant T
 */
interface Statement
{
    /**
     * @return T
     */
    public function raw();
}
