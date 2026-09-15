<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\AST\Transformers\CountModifier;
use Flow\PostgreSql\AST\Transformers\KeysetPaginationConfig;
use Flow\PostgreSql\AST\Transformers\KeysetPaginationModifier;
use Flow\PostgreSql\AST\Transformers\PaginationConfig;
use Flow\PostgreSql\AST\Transformers\PaginationModifier;
use Flow\PostgreSql\Exception\ParserException;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\QueryBuilder\Cursor\DeclareCursorOptionsStep;
use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\declare_cursor;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\sql_parse;
use function sprintf;

final readonly class ReadQuery
{
    private function __construct(
        private string $sql,
        private ParsedQuery $parsed,
        private SelectStatement $select,
    ) {}

    /**
     * @param class-string $extractor
     *
     * @throws ParserException
     * @throws InvalidArgumentException
     */
    public static function of(Sql|string $query, string $extractor): self
    {
        $sql = $query instanceof Sql ? $query->toSql() : $query;
        $parsed = sql_parse($sql);

        try {
            $select = $parsed->statements()->assertReadOnlySelect();
        } catch (InvalidStatementException $e) {
            throw new InvalidArgumentException(
                sprintf('%s reads exactly one read-only SELECT or VALUES statement: %s', $extractor, $e->getMessage()),
                0,
                $e,
            );
        }

        return new self($sql, $parsed, $select);
    }

    public function count(): string
    {
        return $this->copy()->traverse(new CountModifier())->deparse();
    }

    public function declareCursor(string $cursorName): DeclareCursorOptionsStep
    {
        return declare_cursor($cursorName, $this->parsed);
    }

    public function isOrdered(): bool
    {
        return $this->select->hasOrderBy();
    }

    /**
     * The first page: LIMIT $first, no key condition.
     *
     * @param int $first the first placeholder after the caller's parameters
     */
    public function keySetFirstPage(KeySet $keySet, int $first): string
    {
        return $this
            ->copy()
            ->traverse(new KeysetPaginationModifier(
                new KeysetPaginationConfig(param($first), $keySet->toKeysetColumns()),
            ))
            ->deparse();
    }

    /**
     * Every later page: LIMIT $first, and the last row's key values from $first + 1 on.
     *
     * @param int $first the first placeholder after the caller's parameters
     */
    public function keySetNextPage(KeySet $keySet, int $first): string
    {
        return $this
            ->copy()
            ->traverse(new KeysetPaginationModifier(
                new KeysetPaginationConfig(param($first), $keySet->toKeysetColumns(), param($first + 1)),
            ))
            ->deparse();
    }

    /**
     * LIMIT $first OFFSET $first + 1.
     *
     * @param int $first the first placeholder after the caller's parameters
     */
    public function page(int $first): string
    {
        return $this
            ->copy()
            ->traverse(new PaginationModifier(new PaginationConfig(param($first), param($first + 1))))
            ->deparse();
    }

    public function sql(): string
    {
        return $this->sql;
    }

    private function copy(): ParsedQuery
    {
        // the modifiers edit the tree in place, and a deep copy costs a fraction of a parse
        $tree = new ParseResult();
        $tree->mergeFrom($this->parsed->raw());

        return new ParsedQuery($tree);
    }
}
