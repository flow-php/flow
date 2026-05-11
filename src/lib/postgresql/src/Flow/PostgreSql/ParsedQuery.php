<?php

declare(strict_types=1);

namespace Flow\PostgreSql;

use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\Nodes\StatementFactory;
use Flow\PostgreSql\AST\Nodes\Statements;
use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\Protobuf\AST\ParseResult;

final readonly class ParsedQuery
{
    public function __construct(
        private ParseResult $parseResult,
    ) {}

    /**
     * Convert the parsed AST back to SQL string.
     *
     * When called without options, returns the SQL as a simple string.
     * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
     *
     * @throws \RuntimeException if deparsing fails
     */
    public function deparse(?DeparseOptions $options = null): string
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
            $options->commasAtStartOfLine(),
        );
    }

    public function raw(): ParseResult
    {
        return $this->parseResult;
    }

    public function statements(): Statements
    {
        $statements = [];

        foreach ($this->parseResult->getStmts() as $rawStmt) {
            $node = $rawStmt->getStmt();

            if ($node !== null) {
                $statements[] = StatementFactory::fromNode($node);
            }
        }

        return new Statements($statements);
    }

    /**
     * Traverse the AST with visitors and/or modifiers.
     *
     * Visitors collect information (read-only), modifiers mutate nodes.
     * Returns $this to allow method chaining.
     */
    public function traverse(NodeVisitor|NodeModifier ...$handlers): self
    {
        $traverser = new Traverser(...$handlers);
        $traverser->traverse($this->parseResult);

        return $this;
    }
}
