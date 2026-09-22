<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use Flow\PostgreSql\QueryBuilder\Table\SubqueryReference;

use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;

/**
 * Modifies SELECT queries to add LIMIT/OFFSET pagination.
 *
 * Always overrides any existing LIMIT/OFFSET clauses.
 * Validates that OFFSET requires ORDER BY (strict mode).
 *
 * For set operations (UNION/INTERSECT/EXCEPT), the query is wrapped in a
 * subquery to ensure correct semantics:
 * SELECT * FROM (...original query...) AS _pagination_subq LIMIT x OFFSET y
 */
final readonly class PaginationModifier implements NodeModifier
{
    public function __construct(
        private PaginationConfig $config,
    ) {}

    public static function nodeClasses(): array
    {
        return [ParseResult::class, SelectStmt::class];
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        if ($node instanceof ParseResult) {
            (new ParsedQuery($node))->statements()->assertReadOnlySelect();

            return null;
        }

        /** @var SelectStmt $node */
        if (!$context->isTopLevel()) {
            return null;
        }

        if ($this->hasOffset() && !(new SelectStatement($node))->hasOrderBy()) {
            throw new PaginationException('OFFSET without ORDER BY produces non-deterministic results');
        }

        if ((new SelectStatement($node))->hasSetOperation()) {
            return $this->wrapSetOperationWithPagination($node);
        }

        $this->applyPagination($node);

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
    }

    private function applyPagination(SelectStmt $stmt): void
    {
        $stmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $stmt->setLimitCount(
            ($this->config->limit instanceof Parameter ? $this->config->limit : literal($this->config->limit))->toAst(),
        );

        if ($this->hasOffset()) {
            $stmt->setLimitOffset(
                ($this->config->offset instanceof Parameter
                    ? $this->config->offset
                    : literal($this->config->offset))->toAst(),
            );
        } else {
            $stmt->clearLimitOffset();
        }
    }

    private function hasOffset(): bool
    {
        // a parameter's value is known only when the query runs, so it counts as an offset
        return $this->config->offset instanceof Parameter || $this->config->offset > 0;
    }

    private function wrapSetOperationWithPagination(SelectStmt $stmt): Node
    {
        $outerSelect = select(star())
            ->from((new SubqueryReference((new Node())->setSelectStmt($stmt)))->as('_pagination_subq'))
            ->toAst();
        $this->applyPagination($outerSelect);

        return (new Node())->setSelectStmt($outerSelect);
    }
}
