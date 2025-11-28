<?php

declare(strict_types=1);

namespace Flow\PgQuery;

use Flow\PgQuery\AST\Nodes\{Column, FunctionCall, Table};
use Flow\PgQuery\AST\{NodeVisitor, Traverser};
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
     * This method serializes the AST to protobuf binary format and uses
     * libpg_query's deparser to reconstruct the SQL query.
     *
     * @throws \RuntimeException if deparsing fails
     */
    public function deparse() : string
    {
        return \pg_query_deparse($this->parseResult->serializeToString());
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

    public function traverse(NodeVisitor ...$visitors) : void
    {
        $traverser = new Traverser(...$visitors);
        $traverser->traverse($this->parseResult);
    }
}
