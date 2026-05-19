<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\LockingClause as ProtobufLockingClause;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a locking clause (FOR UPDATE, FOR SHARE, etc.).
 */
final readonly class LockingClause implements AstConvertible
{
    /**
     * @param list<string> $tables
     */
    public function __construct(
        private LockStrength $strength,
        private array $tables = [],
        private LockWaitPolicy $waitPolicy = LockWaitPolicy::DEFAULT,
    ) {}

    /**
     * @param list<string> $tables
     */
    public static function forKeyShare(array $tables = []): self
    {
        return new self(LockStrength::KEY_SHARE, $tables);
    }

    /**
     * @param list<string> $tables
     */
    public static function forNoKeyUpdate(array $tables = []): self
    {
        return new self(LockStrength::NO_KEY_UPDATE, $tables);
    }

    /**
     * @param list<string> $tables
     */
    public static function forShare(array $tables = []): self
    {
        return new self(LockStrength::SHARE, $tables);
    }

    /**
     * @param list<string> $tables
     */
    public static function forUpdate(array $tables = []): self
    {
        return new self(LockStrength::UPDATE, $tables);
    }

    public static function fromAst(Node $node): static
    {
        $lockingClause = $node->getLockingClause();

        if ($lockingClause === null) {
            throw InvalidAstException::unexpectedNodeType('LockingClause', 'unknown');
        }

        $strength = LockStrength::fromProtobuf($lockingClause->getStrength());
        $waitPolicy = LockWaitPolicy::fromProtobuf($lockingClause->getWaitPolicy());

        $tables = [];
        $lockedRels = $lockingClause->getLockedRels();

        foreach ($lockedRels as $relNode) {
            $rangeVar = $relNode->getRangeVar();

            if ($rangeVar !== null) {
                $relname = $rangeVar->getRelname();

                if ($relname !== '') {
                    $tables[] = $relname;
                }
            }
        }

        return new self($strength, $tables, $waitPolicy);
    }

    public function nowait(): self
    {
        return new self($this->strength, $this->tables, LockWaitPolicy::NOWAIT);
    }

    public function skipLocked(): self
    {
        return new self($this->strength, $this->tables, LockWaitPolicy::SKIP_LOCKED);
    }

    public function strength(): LockStrength
    {
        return $this->strength;
    }

    /**
     * @return list<string>
     */
    public function tables(): array
    {
        return $this->tables;
    }

    public function toAst(): Node
    {
        $lockingClause = new ProtobufLockingClause();
        $lockingClause->setStrength($this->strength->toProtobuf());
        $lockingClause->setWaitPolicy($this->waitPolicy->toProtobuf());

        if ($this->tables !== []) {
            $lockedRels = [];

            foreach ($this->tables as $table) {
                $rangeVar = new RangeVar();
                $rangeVar->setRelname($table);

                $relNode = new Node();
                $relNode->setRangeVar($rangeVar);

                $lockedRels[] = $relNode;
            }

            $lockingClause->setLockedRels($lockedRels);
        }

        $node = new Node();
        $node->setLockingClause($lockingClause);

        return $node;
    }

    public function waitPolicy(): LockWaitPolicy
    {
        return $this->waitPolicy;
    }
}
