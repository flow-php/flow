<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{ClusterStmt, DefElem, Node, RangeVar};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class ClusterBuilder implements ClusterFinalStep
{
    use AstToSql;

    private function __construct(
        private ?string $table = null,
        private ?string $index = null,
        private bool $verbose = false,
    ) {
    }

    public static function all() : ClusterFinalStep
    {
        return new self();
    }

    public static function create() : ClusterFinalStep
    {
        return new self();
    }

    public function table(string $table) : ClusterFinalStep
    {
        return new self($table, $this->index, $this->verbose);
    }

    public function toAst() : ClusterStmt
    {
        $stmt = new ClusterStmt();

        if ($this->table !== null) {
            $identifier = QualifiedIdentifier::parse($this->table);
            $rangeVar = new RangeVar();

            $schema = $identifier->schema();

            if ($schema !== null) {
                $rangeVar->setSchemaname($schema);
            }

            $rangeVar->setRelname($identifier->name());
            $rangeVar->setInh(true);
            $rangeVar->setRelpersistence('p');

            $stmt->setRelation($rangeVar);
        }

        if ($this->index !== null) {
            $stmt->setIndexname($this->index);
        }

        if ($this->verbose) {
            $defElem = new DefElem();
            $defElem->setDefname('verbose');

            $node = new Node();
            $node->setDefElem($defElem);

            $stmt->setParams([$node]);
        }

        return $stmt;
    }

    public function using(string $index) : ClusterFinalStep
    {
        return new self($this->table, $index, $this->verbose);
    }

    public function verbose() : ClusterFinalStep
    {
        return new self($this->table, $this->index, true);
    }
}
