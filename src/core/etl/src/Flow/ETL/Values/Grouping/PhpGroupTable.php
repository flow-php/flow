<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\Php\ScalarColumn;
use Flow\ETL\Values\Column\Physical\PhysicalFor;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function array_key_exists;
use function array_unique;
use function count;
use function Flow\Types\DSL\type_integer;

final class PhpGroupTable implements GroupTable
{
    /**
     * @var array<string, int> by key
     */
    private array $ids = [];

    private readonly KeyEncoder $keys;

    /**
     * @param list<string> $columns
     */
    public function __construct(array $columns, Schema $declared)
    {
        $this->keys = new KeyEncoder($columns, declared: $declared);
    }

    public function assign(Rows $rows): GroupIds
    {
        $keys = $this->keys->of($rows);
        $firstRows = [];

        foreach (array_unique($keys) as $row => $key) {
            if (!array_key_exists($key, $this->ids)) {
                $this->ids[$key] = count($this->ids);
                $firstRows[] = $row;
            }
        }

        $byRow = [];

        foreach ($keys as $key) {
            $byRow[] = $this->ids[$key];
        }

        return new GroupIds(
            new ScalarColumn(type_integer(), (new PhysicalFor())->physical(type_integer()), $byRow, 0),
            $firstRows,
        );
    }

    public function lookup(Rows $rows): Column
    {
        $byRow = [];
        $nullCount = 0;

        foreach ($this->keys->of($rows) as $key) {
            if (array_key_exists($key, $this->ids)) {
                $byRow[] = $this->ids[$key];
            } else {
                $byRow[] = null;
                $nullCount++;
            }
        }

        return new ScalarColumn(type_integer(), (new PhysicalFor())->physical(type_integer()), $byRow, $nullCount);
    }

    public function count(): int
    {
        return count($this->ids);
    }
}
