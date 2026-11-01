<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Table\SubqueryReference;

use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\select;

/**
 * Wraps in SELECT COUNT(*) FROM (...) and drops ORDER BY, LIMIT and OFFSET.
 */
final readonly class CountModifier implements NodeModifier
{
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

        $this->removeOrderBy($node);
        $this->removeLimitOffset($node);

        return $this->wrapWithCount($node);
    }

    private function removeLimitOffset(SelectStmt $stmt): void
    {
        $stmt->clearLimitCount();
        $stmt->clearLimitOffset();
    }

    private function removeOrderBy(SelectStmt $stmt): void
    {
        $stmt->setSortClause([]);
    }

    private function wrapWithCount(SelectStmt $stmt): Node
    {
        return (new Node())->setSelectStmt(
            select(agg_count())
                ->from((new SubqueryReference((new Node())->setSelectStmt($stmt)))->as('_count_subq'))
                ->toAst(),
        );
    }
}
