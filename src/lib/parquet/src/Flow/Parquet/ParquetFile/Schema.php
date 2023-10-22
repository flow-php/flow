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
            NestedColumn::struct(
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

    public function get(string $flatPath) : Column
    {
        if (\array_key_exists($flatPath, $this->cache)) {
            return $this->cache[$flatPath];
        }

        $getByFlatPath = static function (string $flatPath, array $columns) use (&$getByFlatPath) : ?Column {
            /** @var Column $column */
            foreach ($columns as $column) {
                if ($column instanceof FlatColumn) {
                    if ($column->flatPath() === $flatPath) {
                        return $column;
                    }
                } else {
                    /** @var NestedColumn $column */
                    if ($column->flatPath() === $flatPath) {
                        return $column;
                    }

                    /**
                     * @var null|NestedColumn $nestedColumn
                     *
                     * @psalm-suppress MixedFunctionCall
                     */
                    $nestedColumn = $getByFlatPath($flatPath, $column->children());

                    if ($nestedColumn !== null) {
                        return $nestedColumn;
                    }
                }
            }

            return null;
        };

        $column = $getByFlatPath($flatPath, $this->schemaRoot->children());

        if ($column instanceof Column) {
            $this->cache[$flatPath] = $column;

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
        $index++;

        if ($element->num_children) {
            $children = [];

            for ($i = 0; $i < $element->num_children; $i++) {
                $children = \array_merge($children, self::processSchema($schemaElements, $index));
            }

            return [
                NestedColumn::fromThrift(
                    $element,
                    $children,
                ),
            ];
        }

        return [FlatColumn::fromThrift($element)];
    }
}
