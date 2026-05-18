<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use RuntimeException;

use function sprintf;

final readonly class ParsedSelect implements SelectFinalStep
{
    private SelectStmt $ast;

    public function __construct(string $sql)
    {
        $parser = new Parser();
        $parsed = $parser->parse($sql);

        $stmt = $parsed->raw()->getStmts()[0]->getStmt();

        if ($stmt === null) {
            throw new RuntimeException(sprintf('Failed to parse SQL: "%s".', $sql));
        }

        $selectStmt = $stmt->getSelectStmt();

        if ($selectStmt === null) {
            throw new RuntimeException(sprintf('Expected SELECT statement, got: "%s".', $sql));
        }

        $this->ast = $selectStmt;
    }

    public function toAst(): SelectStmt
    {
        return $this->ast;
    }

    public function toSql(): string
    {
        $node = new Node();
        $node->setSelectStmt($this->ast);

        $rawStmt = new RawStmt(['stmt' => $node]);
        $parseResult = new ParseResult();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
