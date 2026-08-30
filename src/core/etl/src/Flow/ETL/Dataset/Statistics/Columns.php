<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use InvalidArgumentException;

use function array_key_exists;
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
     * @param Entry<mixed> $entry
     */
    /**
     * @param Definition<mixed> $definition
     */
    public function add(Definition $definition, mixed $value): void
    {
        $name = $definition->entry()->name();

        if (!array_key_exists($name, $this->columns)) {
            $this->columns[$name] = new Column($definition, $value);

            return;
        }

        $this->columns[$name]->add($definition, $value);
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
}
