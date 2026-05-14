<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RawStmt;

trait AstToSql
{
    abstract public function toAst(): object;

    public function toSql(): string
    {
        return self::deparseAst($this->toAst());
    }

    private static function deparseAst(object $ast): string
    {
        $nodeKey = self::getNodeKeyForAst($ast);
        $node = new Node([$nodeKey => $ast]);

        $rawStmt = new RawStmt(['stmt' => $node]);
        $parseResult = new ParseResult();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }

    private static function getNodeKeyForAst(object $ast): string
    {
        $className = $ast::class;
        $shortName = \substr($className, (int) \strrpos($className, '\\') + 1);
        $result = \preg_replace('/([a-z])([A-Z])/', '$1_$2', $shortName);

        return \strtolower($result ?? $shortName);
    }
}
