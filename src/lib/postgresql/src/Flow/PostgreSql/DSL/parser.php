<?php

declare(strict_types=1);

namespace Flow\PostgreSql\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\PostgreSql\AST\Transformers\CountModifier;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\AST\Transformers\ExplainModifier;
use Flow\PostgreSql\AST\Transformers\KeysetColumn;
use Flow\PostgreSql\AST\Transformers\KeysetPaginationConfig;
use Flow\PostgreSql\AST\Transformers\KeysetPaginationModifier;
use Flow\PostgreSql\AST\Transformers\PaginationConfig;
use Flow\PostgreSql\AST\Transformers\PaginationModifier;
use Flow\PostgreSql\AST\Transformers\SortOrder;
use Flow\PostgreSql\DeparseOptions;
use Flow\PostgreSql\Explain\Analyzer\PlanAnalyzer;
use Flow\PostgreSql\Explain\ExplainParser;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Extractors\Columns;
use Flow\PostgreSql\Extractors\Functions;
use Flow\PostgreSql\Extractors\OrderBy as OrderByExtractor;
use Flow\PostgreSql\Extractors\QueryDepth;
use Flow\PostgreSql\Extractors\Tables;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_parser(): Parser
{
    return new Parser();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_parse(string $sql): ParsedQuery
{
    return (new Parser())->parse($sql);
}

/**
 * Returns a fingerprint of the given SQL query.
 * Literal values are normalized so they won't affect the fingerprint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_fingerprint(string $sql): ?string
{
    return (new Parser())->fingerprint($sql);
}

/**
 * Normalize SQL query by replacing literal values and named parameters with positional parameters.
 * WHERE id = :id will be changed into WHERE id = $1
 * WHERE id = 1 will be changed into WHERE id = $1.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_normalize(string $sql): ?string
{
    return (new Parser())->normalize($sql);
}

/**
 * Normalize utility SQL statements (DDL like CREATE, ALTER, DROP).
 * This handles DDL statements differently from pg_normalize() which is optimized for DML.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_normalize_utility(string $sql): ?string
{
    return (new Parser())->normalizeUtility($sql);
}

/**
 * Split string with multiple SQL statements into array of individual statements.
 *
 * @return array<string>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_split(string $sql): array
{
    return (new Parser())->split($sql);
}

/**
 * Create DeparseOptions for configuring SQL formatting.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_deparse_options(): DeparseOptions
{
    return DeparseOptions::new();
}

/**
 * Convert a ParsedQuery AST back to SQL string.
 *
 * When called without options, returns the SQL as a simple string.
 * When called with DeparseOptions, applies formatting (pretty-printing, indentation, etc.).
 *
 * @throws \RuntimeException if deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_deparse(ParsedQuery $query, ?DeparseOptions $options = null): string
{
    return $query->deparse($options);
}

/**
 * Parse and format SQL query with pretty printing.
 *
 * This is a convenience function that parses SQL and returns it formatted.
 *
 * @param string $sql The SQL query to format
 * @param null|DeparseOptions $options Formatting options (defaults to pretty-print enabled)
 *
 * @throws \RuntimeException if parsing or deparsing fails
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_format(string $sql, ?DeparseOptions $options = null): string
{
    return (new Parser())
        ->parse($sql)
        ->deparse($options ?? DeparseOptions::new());
}

/**
 * Generate a summary of parsed queries in protobuf format.
 * Useful for query monitoring and logging without full AST overhead.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_summary(string $sql, int $options = 0, int $truncateLimit = 0): string
{
    return (new Parser())->summary($sql, $options, $truncateLimit);
}

/**
 * Transform a SQL query into a paginated query with LIMIT and OFFSET.
 *
 * @param string $sql The SQL query to paginate
 * @param int $limit Maximum number of rows to return
 * @param int $offset Number of rows to skip (requires ORDER BY in query)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_paginated_query(string $sql, int $limit, int $offset = 0): string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new PaginationModifier(new PaginationConfig($limit, $offset)));

    return $query->deparse();
}

/**
 * Transform a SQL query to limit results to a specific number of rows.
 *
 * @param string $sql The SQL query to limit
 * @param int $limit Maximum number of rows to return
 *
 * @return string The limited SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_limited_query(string $sql, int $limit): string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new PaginationModifier(new PaginationConfig($limit)));

    return $query->deparse();
}

/**
 * Transform a SQL query into a COUNT query for pagination.
 *
 * Wraps the query in: SELECT COUNT(*) FROM (...) AS _count_subq
 * Removes ORDER BY and LIMIT/OFFSET from the inner query.
 *
 * @param string $sql The SQL query to transform
 *
 * @return string The COUNT query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_count_query(string $sql): string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new CountModifier());

    return $query->deparse();
}

/**
 * Transform a SQL query into a keyset (cursor-based) paginated query.
 *
 * More efficient than OFFSET for large datasets - uses indexed WHERE conditions.
 * Automatically detects existing query parameters and appends keyset placeholders at the end.
 *
 * @param string $sql The SQL query to paginate (must have ORDER BY)
 * @param int $limit Maximum number of rows to return
 * @param list<KeysetColumn> $columns Columns for keyset pagination (must match ORDER BY)
 * @param null|list<null|bool|float|int|string> $cursor Values from last row of previous page (null for first page)
 *
 * @return string The paginated SQL query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_keyset_query(string $sql, int $limit, array $columns, ?array $cursor = null): string
{
    $query = (new Parser())->parse($sql);
    $query->traverse(new KeysetPaginationModifier(new KeysetPaginationConfig($limit, $columns, $cursor)));

    return $query->deparse();
}

/**
 * Create a KeysetColumn for keyset pagination.
 *
 * @param string $column Column name (can include table alias like "u.id")
 * @param SortOrder $order Sort order (ASC or DESC)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_keyset_column(string $column, SortOrder $order = SortOrder::ASC): KeysetColumn
{
    return new KeysetColumn($column, $order);
}

/**
 * Extract columns from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_columns(ParsedQuery $query): Columns
{
    return new Columns($query);
}

/**
 * Extract tables from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_tables(ParsedQuery $query): Tables
{
    return new Tables($query);
}

/**
 * Extract functions from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_functions(ParsedQuery $query): Functions
{
    return new Functions($query);
}

/**
 * Extract ORDER BY clauses from a parsed SQL query.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_order_by(ParsedQuery $query): OrderByExtractor
{
    return new OrderByExtractor($query);
}

/**
 * Get the maximum nesting depth of a SQL query.
 *
 * Example:
 * - "SELECT * FROM t" => 1
 * - "SELECT * FROM (SELECT * FROM t)" => 2
 * - "SELECT * FROM (SELECT * FROM (SELECT * FROM t))" => 3
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_query_depth(string $sql): int
{
    return (new QueryDepth(sql_parse($sql)))->depth();
}

/**
 * Transform a SQL query into an EXPLAIN query.
 *
 * Returns the modified SQL with EXPLAIN wrapped around it.
 * Defaults to EXPLAIN ANALYZE with JSON format for easy parsing.
 *
 * @param string $sql The SQL query to explain
 * @param null|ExplainConfig $config EXPLAIN configuration (defaults to forAnalysis())
 *
 * @return string The EXPLAIN query
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_to_explain(string $sql, ?ExplainConfig $config = null): string
{
    $config ??= ExplainConfig::forAnalysis();
    $query = (new Parser())->parse($sql);
    $query->traverse(new ExplainModifier($config));

    return $query->deparse();
}

/**
 * Create an ExplainConfig for customizing EXPLAIN options.
 *
 * @param bool $analyze Whether to actually execute the query (ANALYZE)
 * @param bool $verbose Include verbose output
 * @param bool $costs Include cost estimates (default true)
 * @param bool $buffers Include buffer usage statistics (requires analyze)
 * @param bool $timing Include timing information (requires analyze)
 * @param ExplainFormat $format Output format (JSON recommended for parsing)
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_config(
    bool $analyze = true,
    bool $verbose = false,
    bool $costs = true,
    bool $buffers = true,
    bool $timing = true,
    ExplainFormat $format = ExplainFormat::JSON,
): ExplainConfig {
    return new ExplainConfig(
        analyze: $analyze,
        verbose: $verbose,
        costs: $costs,
        buffers: $buffers,
        timing: $timing,
        format: $format,
    );
}

/**
 * Create an ExplainModifier for transforming queries into EXPLAIN queries.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_modifier(ExplainConfig $config): ExplainModifier
{
    return new ExplainModifier($config);
}

/**
 * Parse EXPLAIN JSON output into a Plan object.
 *
 * @param string $jsonOutput The JSON output from EXPLAIN (FORMAT JSON)
 *
 * @return Plan The parsed execution plan
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_explain_parse(string $jsonOutput): Plan
{
    return (new ExplainParser())->parse($jsonOutput);
}

/**
 * Create a plan analyzer for analyzing EXPLAIN plans.
 *
 * @param Plan $plan The execution plan to analyze
 *
 * @return PlanAnalyzer The analyzer for extracting insights
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function sql_analyze(Plan $plan): PlanAnalyzer
{
    return new PlanAnalyzer($plan);
}
