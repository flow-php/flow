<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics\Statistics;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Column as ColumnData;
use Flow\ETL\Values\Schema\Definition;
use InvalidArgumentException;

use function array_key_exists;
use function array_map;
use function array_values;
use function sprintf;

final class Columns
{
    /**
     * @var array<string, Column>
     */
    private array $columns = [];

    public function __construct() {}

    /**
     * @param Definition<mixed> $definition
     */
    public function add(Definition $definition, ColumnData $column): void
    {
        $name = $definition->name();

        if (!array_key_exists($name, $this->columns)) {
            $this->columns[$name] = new Column($definition, $column);

            return;
        }

        $this->columns[$name]->add($definition, $column);
    }

    /**
     * @return array<Column>
     */
    public function all(): array
    {
        return array_values($this->columns);
    }

    public function get(string|Reference $ref): Column
    {
        if ($ref instanceof Reference) {
            $ref = $ref->name();
        }

        if (!array_key_exists($ref, $this->columns)) {
            throw new InvalidArgumentException(sprintf('Column "%s" does not exist.', $ref));
        }

        return $this->columns[$ref];
    }

    /**
     * @return list<array<string, null|bool|float|int|string>>
     */
    public function normalize(): array
    {
        return array_map(static fn(Column $column): array => $column->normalize(), array_values($this->columns));
    }
}
