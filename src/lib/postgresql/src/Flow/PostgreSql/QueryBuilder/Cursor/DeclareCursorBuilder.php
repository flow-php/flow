<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\DeclareCursorStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Sql;

final class DeclareCursorBuilder implements DeclareCursorOptionsStep
{
    use AstToSql;

    private int $options = CursorOption::NO_SCROLL;

    private function __construct(
        private readonly string $cursorName,
        private readonly Node $queryNode,
    ) {}

    public static function create(string $cursorName, SelectFinalStep $query): DeclareCursorOptionsStep
    {
        $node = new Node();
        $node->setSelectStmt($query->toAst());

        return new self($cursorName, $node);
    }

    public static function createFromSql(string $cursorName, string|Sql $query): DeclareCursorOptionsStep
    {
        $sql = $query instanceof Sql ? $query->toSql() : $query;
        $parser = new Parser();
        $parsed = $parser->parse($sql);
        $rawStmts = $parsed->raw()->getStmts();

        if (\count($rawStmts) === 0) {
            throw new \InvalidArgumentException('Query cannot be empty');
        }

        $firstStmt = $rawStmts[0];
        $stmtNode = $firstStmt->getStmt();

        if ($stmtNode === null) {
            throw new \InvalidArgumentException('Invalid query: no statement found');
        }

        return new self($cursorName, $stmtNode);
    }

    public function binary(): DeclareCursorOptionsStep
    {
        $this->options |= CursorOption::BINARY;

        return $this;
    }

    public function noScroll(): DeclareCursorOptionsStep
    {
        $this->options |= CursorOption::NO_SCROLL;
        $this->options &= ~CursorOption::SCROLL;

        return $this;
    }

    public function scroll(): DeclareCursorOptionsStep
    {
        $this->options |= CursorOption::SCROLL;
        $this->options &= ~CursorOption::NO_SCROLL;

        return $this;
    }

    public function toAst(): DeclareCursorStmt
    {
        $stmt = new DeclareCursorStmt();
        $stmt->setPortalname($this->cursorName);
        $stmt->setOptions($this->options);
        $stmt->setQuery($this->queryNode);

        return $stmt;
    }

    public function withHold(): DeclareCursorOptionsStep
    {
        $this->options |= CursorOption::HOLD;

        return $this;
    }
}
