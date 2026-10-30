<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RawStmt;

use function preg_replace;
use function strrpos;
use function strtolower;
use function substr;

final readonly class AstDeparser
{
    public function deparse(object $ast): string
    {
        $shortName = substr($ast::class, (int) strrpos($ast::class, '\\') + 1);
        $parseResult = new ParseResult();
        $parseResult->setStmts([
            new RawStmt([
                'stmt' => new Node([
                    strtolower(preg_replace('/([a-z])([A-Z])/', '$1_$2', $shortName) ?? $shortName) => $ast,
                ]),
            ]),
        ]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
