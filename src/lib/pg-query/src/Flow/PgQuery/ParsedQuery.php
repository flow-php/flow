<?php

declare(strict_types=1);

namespace Flow\PgQuery;

use Flow\PgQuery\AST\{NodeModifier, NodeVisitor, Traverser};
use Flow\PgQuery\AST\Nodes\{Column, FunctionCall, Table};
use Flow\PgQuery\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PgQuery\Protobuf\AST\ParseResult;

final readonly class ParsedQuery
{
    public function __construct(
        private ParseResult $parseResult,
    ) {
    }

    /**
     * @return array<Column>
     */
    public function columns(?string $tableName = null) : array
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseResult);

        $columns = [];

        foreach ($collector->getColumnRefs() as $columnRef) {
            $column = new Column($columnRef);

            if ($column->name() === null) {
                continue;
            }

            if ($tableName !== null && $column->table() !== $tableName) {
                continue;
            }

            $columns[] = $column;
        }

        return $columns;
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

    /**
     * @return array<FunctionCall>
     */
    public function functions() : array
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseResult);

        $functions = [];

        foreach ($collector->getFuncCalls() as $funcCall) {
            $function = new FunctionCall($funcCall);

            if ($function->name() === null) {
                continue;
            }

            $functions[] = $function;
        }

        return $functions;
    }

    public function raw() : ParseResult
    {
        return $this->parseResult;
    }

    /**
     * @return array<Table>
     */
    public function tables() : array
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseResult);

        $tables = [];

        foreach ($collector->getRangeVars() as $rangeVar) {
            $table = new Table($rangeVar);

            if ($table->name() === '') {
                continue;
            }

            $tables[] = $table;
        }

        return $tables;
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
