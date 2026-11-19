<?php

declare(strict_types=1);

namespace Flow\PostgreSql;

use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\Nodes\StatementFactory;
use Flow\PostgreSql\AST\Nodes\Statements;
use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\Exception\ExtensionOutdatedException;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use ReflectionFunction;

use function pg_query_deparse;
use function pg_query_deparse_opts;

final readonly class ParsedQuery
{
    public function __construct(
        private ParseResult $parseResult,
        private ?string $sql = null,
    ) {}

    /**
     * Convert the parsed AST back to SQL string.
     *
     * When called without options, returns the SQL as a simple string.
     * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
     *
     * @throws ExtensionOutdatedException if comments are requested and the loaded extension cannot keep them
     * @throws \RuntimeException if deparsing fails
     */
    public function deparse(?DeparseOptions $options = null): string
    {
        if ($options === null) {
            return pg_query_deparse($this->parseResult->serializeToString());
        }

        $arguments = [
            $this->parseResult->serializeToString(),
            $options->hasPrettyPrint(),
            $options->getIndentSize(),
            $options->getMaxLineLength(),
            $options->hasTrailingNewline(),
            $options->commasAtStartOfLine(),
        ];

        if ($options->hasPreserveComments() && $this->sql !== null) {
            if ($this->parseResult->getStmts()->count() === 0) {
                return $this->sql;
            }

            if ((new ReflectionFunction('pg_query_deparse_opts'))->getNumberOfParameters() < 7) {
                throw new ExtensionOutdatedException();
            }

            $arguments[] = $this->sql;
        }

        return pg_query_deparse_opts(...$arguments);
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
