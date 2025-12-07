<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{ClusterStmt, DefElem, Node, RangeVar};

final readonly class ClusterBuilder implements ClusterFinalStep
{
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
            $rangeVar = new RangeVar();

            $parts = \explode('.', $this->table);

            if (\count($parts) === 2) {
                $rangeVar->setSchemaname($parts[0]);
                $rangeVar->setRelname($parts[1]);
            } else {
                $rangeVar->setRelname($this->table);
            }

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
