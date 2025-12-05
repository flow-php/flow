<?php

declare(strict_types=1);

namespace Flow\PgQuery;

use Flow\PgQuery\AST\{NodeModifier, NodeVisitor, Traverser};
use Flow\PgQuery\Protobuf\AST\{Node, ParseResult};
use Flow\PgQuery\QueryBuilder\Delete\DeleteBuilder;
use Flow\PgQuery\QueryBuilder\Insert\InsertBuilder;
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Update\UpdateBuilder;

final readonly class ParsedQuery
{
    public function __construct(
        private ParseResult $parseResult,
    ) {
    }

    /**
     * Convert the parsed AST back to SQL string.
     *
     * When called without options, returns the SQL as a simple string.
     * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
     *
     * @throws \RuntimeException if deparsing fails
     */
    public function deparse(?DeparseOptions $options = null) : string
    {
        if ($options === null) {
            return \pg_query_deparse($this->parseResult->serializeToString());
        }

        return \pg_query_deparse_opts(
            $this->parseResult->serializeToString(),
            $options->hasPrettyPrint(),
            $options->getIndentSize(),
            $options->getMaxLineLength(),
            $options->hasTrailingNewline(),
            $options->commasAtStartOfLine()
        );
    }

    public function raw() : ParseResult
    {
        return $this->parseResult;
    }

    /**
     * Convert parsed query to a DeleteBuilder.
     *
     * @throws \InvalidArgumentException if query is not a DELETE statement
     */
    public function toDeleteBuilder() : DeleteBuilder
    {
        $node = $this->getSingleStatementNode();

        if ($node->getDeleteStmt() === null) {
            throw new \InvalidArgumentException('Query is not a DELETE statement');
        }

        return DeleteBuilder::fromAst($node);
    }

    /**
     * Convert parsed query to an InsertBuilder.
     *
     * @throws \InvalidArgumentException if query is not an INSERT statement
     */
    public function toInsertBuilder() : InsertBuilder
    {
        $node = $this->getSingleStatementNode();

        if ($node->getInsertStmt() === null) {
            throw new \InvalidArgumentException('Query is not an INSERT statement');
        }

        return InsertBuilder::fromAst($node);
    }

    /**
     * Convert parsed query to a QueryBuilder.
     *
     * Only works for single-statement queries. For multiple statements,
     * use pg_split() to parse statements individually.
     *
     * @throws \InvalidArgumentException if query contains multiple statements or unsupported statement type
     */
    public function toQueryBuilder() : SelectBuilder|InsertBuilder|UpdateBuilder|DeleteBuilder
    {
        $node = $this->getSingleStatementNode();

        return match (true) {
            $node->getSelectStmt() !== null => SelectBuilder::fromAst($node),
            $node->getInsertStmt() !== null => InsertBuilder::fromAst($node),
            $node->getUpdateStmt() !== null => UpdateBuilder::fromAst($node),
            $node->getDeleteStmt() !== null => DeleteBuilder::fromAst($node),
            default => throw new \InvalidArgumentException('Unsupported statement type'),
        };
    }

    /**
     * Convert parsed query to a SelectBuilder.
     *
     * @throws \InvalidArgumentException if query is not a SELECT statement
     */
    public function toSelectBuilder() : SelectBuilder
    {
        $node = $this->getSingleStatementNode();

        if ($node->getSelectStmt() === null) {
            throw new \InvalidArgumentException('Query is not a SELECT statement');
        }

        return SelectBuilder::fromAst($node);
    }

    /**
     * Convert parsed query to an UpdateBuilder.
     *
     * @throws \InvalidArgumentException if query is not an UPDATE statement
     */
    public function toUpdateBuilder() : UpdateBuilder
    {
        $node = $this->getSingleStatementNode();

        if ($node->getUpdateStmt() === null) {
            throw new \InvalidArgumentException('Query is not an UPDATE statement');
        }

        return UpdateBuilder::fromAst($node);
    }

    /**
     * Traverse the AST with visitors and/or modifiers.
     *
     * Visitors collect information (read-only), modifiers mutate nodes.
     * Returns $this to allow method chaining.
     */
    public function traverse(NodeVisitor|NodeModifier ...$handlers) : self
    {
        $traverser = new Traverser(...$handlers);
        $traverser->traverse($this->parseResult);

        return $this;
    }

    /**
     * @throws \InvalidArgumentException if no statements or multiple statements found
     */
    private function getSingleStatementNode() : Node
    {
        $stmts = $this->parseResult->getStmts();
        $count = \count($stmts);

        if ($count === 0) {
            throw new \InvalidArgumentException('No statements found');
        }

        if ($count > 1) {
            throw new \InvalidArgumentException(
                'Multiple statements found. Use pg_split() to parse statements individually.'
            );
        }

        $node = $stmts[0]->getStmt();

        if ($node === null) {
            throw new \InvalidArgumentException('Statement has no node');
        }

        return $node;
    }
}
