<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

/**
 * Extracts bucket-key values positionally, in reference order - position, not column name, defines
 * key identity, so two sides of a join extract hash-compatible values from differently named columns.
 */
final readonly class KeyValues
{
    /**
     * @param list<Reference> $refs
     */
    public function __construct(
        private array $refs,
    ) {}

    /**
     * @return list<list<mixed>>
     */
    public function of(Rows $rows): array
    {
        $values = [];
        $schema = $rows->schema();

        foreach ($rows as $row) {
            $values[] = $this->ofRow($row, $schema);
        }

        return $values;
    }

    /**
     * @return list<mixed>
     */
    public function ofRow(Row $row, Schema $schema): array
    {
        $values = [];

        foreach ($this->refs as $ref) {
            // absent under a nullable declaration is a legitimate null - it keys into a bucket that
            // no comparison will match, which is what SQL says an unknown key does. Absent under NOT
            // NULL is a row-shape violation, and Row::get() names it and lists the available columns.
            $values[] = !$row->has($ref) && $schema->get($ref)->isNullable() ? null : $row->get($ref);
        }

        return $values;
    }
}
