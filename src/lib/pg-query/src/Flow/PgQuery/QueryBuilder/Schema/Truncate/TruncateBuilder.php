<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Truncate;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, Node, RangeVar, TruncateStmt};

final readonly class TruncateBuilder implements TruncateFinalStep
{
    /**
     * @param list<string> $tables
     */
    private function __construct(
        private array $tables,
        private bool $restartIdentity = false,
        private int $behavior = DropBehavior::DROP_BEHAVIOR_UNDEFINED,
    ) {
    }

    public static function create(string ...$tables) : TruncateFinalStep
    {
        return new self(\array_values($tables));
    }

    public function cascade() : TruncateFinalStep
    {
        return new self(
            $this->tables,
            $this->restartIdentity,
            DropBehavior::DROP_CASCADE,
        );
    }

    public function continueIdentity() : TruncateFinalStep
    {
        return new self(
            $this->tables,
            false,
            $this->behavior,
        );
    }

    public function restartIdentity() : TruncateFinalStep
    {
        return new self(
            $this->tables,
            true,
            $this->behavior,
        );
    }

    public function restrict() : TruncateFinalStep
    {
        return new self(
            $this->tables,
            $this->restartIdentity,
            DropBehavior::DROP_RESTRICT,
        );
    }

    public function toAst() : TruncateStmt
    {
        $stmt = new TruncateStmt();

        if ($this->restartIdentity) {
            $stmt->setRestartSeqs(true);
        }

        if ($this->behavior !== DropBehavior::DROP_BEHAVIOR_UNDEFINED) {
            $stmt->setBehavior($this->behavior);
        }

        $relations = [];

        foreach ($this->tables as $table) {
            $relations[] = $this->createRangeVarNode($table);
        }

        $stmt->setRelations($relations);

        return $stmt;
    }

    private function createRangeVarNode(string $table) : Node
    {
        $parts = \explode('.', $table);

        $rangeVar = new RangeVar();
        $rangeVar->setRelpersistence('p');
        $rangeVar->setInh(true);

        if (\count($parts) === 2) {
            $rangeVar->setSchemaname($parts[0]);
            $rangeVar->setRelname($parts[1]);
        } else {
            $rangeVar->setRelname($parts[0]);
        }

        $node = new Node();
        $node->setRangeVar($rangeVar);

        return $node;
    }
}
