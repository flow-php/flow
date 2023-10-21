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

    /**
     * @param array<Column> $columns
     */
    private function __construct(
        private readonly FlatColumn $schemaRoot,
        private readonly array $columns
    ) {
    }

    public static function from(Column ...$columns) : self
    {
        return new self(
            FlatColumn::boolean('schema'),
            $columns
        );
    }

    /**
     * @param array<SchemaElement> $schemaElements
     */
    public static function fromThrift(array $schemaElements) : self
    {
        if (!\count($schemaElements)) {
            throw new \InvalidArgumentException('Schema must have at least one element');
        }

        return new self(
            FlatColumn::fromThrift(\array_shift($schemaElements)),
            self::processSchema($schemaElements)
        );
    }

    /**
     * @return array<Column>
     */
    public function columns() : array
    {
        return $this->columns;
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

        $column = $getByFlatPath($flatPath, $this->columns);

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
            'children' => $this->generateDDL($this->columns),
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
    private static function processSchema(
        array $schemaElements,
        int &$index = 0,
        ?string $rootPath = null,
        int $childrenCount = null,
        Column $parent = null
    ) : array {
        $columns = [];

        $processedChildren = 0;

        while ($index < \count($schemaElements) && ($childrenCount === null || $processedChildren < $childrenCount)) {
            $elem = $schemaElements[$index];
            $index++;

            $root = FlatColumn::fromThrift($elem, $rootPath, $parent);

            if ($elem->num_children > 0) {
                $nestedColumn = new NestedColumn($root, [], $parent);
                $children = self::processSchema(
                    $schemaElements,
                    $index,
                    $root->flatPath(),
                    $elem->num_children,
                    $nestedColumn
                );

                // now update the children of the NestedColumn
                $nestedColumn->setChildren($children);

                $columns[] = $nestedColumn;  // use the updated NestedColumn
            } else {
                $columns[] = $root;
            }

            $processedChildren++;
        }

        return $columns;
    }
}
