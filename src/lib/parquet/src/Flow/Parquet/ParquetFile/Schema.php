<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Thrift\SchemaElement;

final class Schema
{
    /**
     * @var array<string, Column>
     */
    private array $cache = [];

    public function __construct(
        private readonly NestedColumn $schemaRoot,
    ) {
    }

    /**
     * @param array<SchemaElement> $schemaElements
     */
    public static function fromThrift(array $schemaElements) : self
    {
        if (!\count($schemaElements)) {
            throw new InvalidArgumentException('Schema must have at least one element');
        }

        $schema = self::processSchema($schemaElements);

        if (\count($schema) !== 1) {
            throw new InvalidArgumentException('Schema must have exactly one root element');
        }

        if (!$schema[0] instanceof NestedColumn) {
            throw new InvalidArgumentException('Schema must be a NestedColumn');
        }

        return new self($schema[0]);
    }

    public static function with(Column ...$columns) : self
    {
        return new self(
            NestedColumn::schemaRoot(
                'schema',
                $columns,
            )
        );
    }

    /**
     * @return array<Column>
     */
    public function columns() : array
    {
        return $this->schemaRoot->children();
    }

    /**
     * @return array<FlatColumn>
     */
    public function columnsFlat() : array
    {
        $columns = [];

        foreach ($this->schemaRoot->children() as $column) {
            $columns = \array_merge($columns, $this->flattener($column));
        }

        return $columns;
    }

    public function get(string $flatPath) : Column
    {
        if (!\count($this->cache)) {
            foreach ($this->columns() as $column) {
                $this->cache($column);
            }
        }

        if (\array_key_exists($flatPath, $this->cache)) {
            return $this->cache[$flatPath];
        }

        throw new InvalidArgumentException("Column \"{$flatPath}\" does not exist");
    }

    public function has(string $name) : bool
    {
        try {
            $this->get($name);

            return true;
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }

    public function toDDL() : array
    {
        return [$this->schemaRoot->name() => [
            'type' => 'message',
            'children' => $this->generateDDL($this->schemaRoot->children()),
        ]];
    }

    public function toThrift() : array
    {
        return $this->schemaRoot->toThrift();
    }

    private function cache(Column $column) : void
    {
        $this->cache[$column->flatPath()] = $column;

        if ($column instanceof NestedColumn) {
            foreach ($column->children() as $child) {
                $this->cache($child);
            }
        }
    }

    /**
     * @return array<FlatColumn>
     */
    private function flattener(Column $column) : array
    {
        if ($column instanceof FlatColumn) {
            return [$column];
        }

        /** @var NestedColumn $column */
        $columns = [];

        foreach ($column->children() as $child) {
            $columns = \array_merge($columns, $this->flattener($child));
        }

        return $columns;
    }

    /**
     * @param array<Column> $columns
     */
    private function generateDDL(array $columns) : array
    {
        $ddlArray = [];

        foreach ($columns as $column) {
            $ddlArray[$column->name()] = $column->ddl();
        }

        return $ddlArray;
    }

    /**
     * @param array<SchemaElement> $schemaElements
     *
     * @return array<Column>
     */
    private static function processSchema(array $schemaElements, int &$index = 0) : array
    {
        $element = $schemaElements[$index];
        $schemaRoot = $index === 0;
        $index++;

        if ($element->num_children) {
            $children = [];

            for ($i = 0; $i < $element->num_children; $i++) {
                $children = \array_merge($children, self::processSchema($schemaElements, $index));
            }

            return [
                $schemaRoot
                    ? NestedColumn::schemaRoot(
                        $element->name,
                        $children,
                    )
                    : NestedColumn::fromThrift(
                        $element,
                        $children,
                    ),
            ];
        }

        return [FlatColumn::fromThrift($element)];
    }
}
