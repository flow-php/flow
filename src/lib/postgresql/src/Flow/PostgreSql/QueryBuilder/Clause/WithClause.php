<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{Node, WithClause as ProtobufWithClause};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a WITH clause containing one or more CTEs.
 */
final readonly class WithClause
{
    /**
     * @param array<CTE> $ctes
     */
    public function __construct(
        private array $ctes,
        private bool $recursive = false,
    ) {
    }

    public static function fromAst(ProtobufWithClause $withClause) : static
    {
        $cteNodes = $withClause->getCtes();

        if ($cteNodes === null || \count($cteNodes) === 0) {
            throw InvalidAstException::missingRequiredField('ctes', 'WithClause');
        }

        $ctes = [];

        foreach ($cteNodes as $cteNode) {
            $ctes[] = CTE::fromAst($cteNode);
        }

        $recursive = $withClause->getRecursive();

        return new self($ctes, $recursive);
    }

    public function add(CTE $cte) : self
    {
        return new self([...$this->ctes, $cte], $this->recursive);
    }

    /**
     * @return array<CTE>
     */
    public function ctes() : array
    {
        return $this->ctes;
    }

    public function recursive() : bool
    {
        return $this->recursive;
    }

    public function toAst() : Node
    {
        $withClause = new ProtobufWithClause();
        $withClause->setRecursive($this->recursive);

        $cteNodes = [];

        foreach ($this->ctes as $cte) {
            $cteNodes[] = $cte->toAst();
        }

        $withClause->setCtes($cteNodes);

        $node = new Node();
        $node->setWithClause($withClause);

        return $node;
    }
}
