<?php

declare(strict_types=1);

namespace Flow\PgQuery;

use Flow\PgQuery\AST\{NodeModifier, NodeVisitor, Traverser};
use Flow\PgQuery\Protobuf\AST\ParseResult;

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
}
