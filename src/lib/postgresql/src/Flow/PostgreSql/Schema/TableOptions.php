<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;

/**
 * Table-level options the {@see \Flow\ETL\Adapter\PostgreSql\SchemaConverter} does not derive from a
 * Flow Schema: storage options plus the schema-element collections that have no Flow-Schema equivalent.
 */
final readonly class TableOptions
{
    /**
     * @param list<ForeignKey> $foreignKeys
     * @param list<CheckConstraint> $checkConstraints
     * @param list<ExcludeConstraint> $excludeConstraints
     * @param list<Trigger> $triggers
     * @param list<string> $partitionColumns
     * @param list<string> $inherits
     */
    public function __construct(
        public array $foreignKeys = [],
        public array $checkConstraints = [],
        public array $excludeConstraints = [],
        public array $triggers = [],
        public bool $unlogged = false,
        public ?PartitionStrategy $partitionStrategy = null,
        public array $partitionColumns = [],
        public array $inherits = [],
        public ?string $tablespace = null,
    ) {}
}
