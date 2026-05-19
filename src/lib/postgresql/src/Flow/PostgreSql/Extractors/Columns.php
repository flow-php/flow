<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Extractors;

use Flow\PostgreSql\AST\Nodes\Column;
use Flow\PostgreSql\AST\Visitors\ColumnRefCollector;
use Flow\PostgreSql\ParsedQuery;

use function array_filter;
use function array_map;
use function array_values;

final readonly class Columns
{
    public function __construct(
        private ParsedQuery $query,
    ) {}

    /**
     * @return array<Column>
     */
    public function all(): array
    {
        $collector = new ColumnRefCollector();
        $this->query->traverse($collector);

        return array_values(array_filter(
            array_map(static fn($ref) => new Column($ref), $collector->getColumnRefs()),
            static fn($col) => $col->name() !== null,
        ));
    }

    /**
     * @return array<Column>
     */
    public function forTable(string $tableName): array
    {
        return array_values(array_filter($this->all(), static fn($col) => $col->table() === $tableName));
    }
}
