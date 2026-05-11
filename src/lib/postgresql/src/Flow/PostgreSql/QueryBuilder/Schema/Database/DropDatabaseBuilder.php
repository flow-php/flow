<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Database;

use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\DropdbStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class DropDatabaseBuilder implements DropDatabaseFinalStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private bool $ifExists = false,
        private bool $force = false,
    ) {}

    public static function create(string $name): DropDatabaseFinalStep
    {
        return new self($name);
    }

    public function force(): self
    {
        return new self($this->name, $this->ifExists, true);
    }

    public function ifExists(): self
    {
        return new self($this->name, true, $this->force);
    }

    public function toAst(): DropdbStmt
    {
        $stmt = new DropdbStmt();
        $stmt->setDbname($this->name);
        $stmt->setMissingOk($this->ifExists);

        if ($this->force) {
            $defElem = new DefElem();
            $defElem->setDefname('force');
            $node = new Node();
            $node->setDefElem($defElem);
            $stmt->setOptions([$node]);
        }

        return $stmt;
    }
}
