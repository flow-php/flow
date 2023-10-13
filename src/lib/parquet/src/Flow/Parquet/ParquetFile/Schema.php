<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
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
    public function __construct(
        private readonly FlatColumn $schemaRoot,
        private readonly array $columns
    ) {
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
            FlatColumn::fromThrift(\array_shift($schemaElements), 0, 0),
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
        int $maxDefinitionLevel = 0,
        int $maxRepetitionLevel = 0,
        int $childrenCount = null,
        Column $parent = null
    ) : array {
        $columns = [];

        $processed_children = 0;

        while ($index < \count($schemaElements) && ($childrenCount === null || $processed_children < $childrenCount)) {
            $elem = $schemaElements[$index];
            $index++;

            $currentMaxDefLevel = $maxDefinitionLevel;
            $currentMaxRepLevel = $maxRepetitionLevel;

            // Update maxDefinitionLevel and maxRepetitionLevel based on the repetition type of the current element
            if ($elem->repetition_type !== Repetition::REQUIRED->value) {
                $currentMaxDefLevel++;
            }

            if ($elem->repetition_type === Repetition::REPEATED->value) {
                $currentMaxRepLevel++;
            }

            $root = FlatColumn::fromThrift($elem, $currentMaxDefLevel, $currentMaxRepLevel, $rootPath, $parent);

            if ($elem->num_children > 0) {
                $nestedColumn = new NestedColumn($root, [], $currentMaxDefLevel, $currentMaxRepLevel, $parent);  // create NestedColumn with empty children first
                $children = self::processSchema(
                    $schemaElements,
                    $index,
                    $root->flatPath(),
                    $currentMaxDefLevel,
                    $currentMaxRepLevel,
                    $elem->num_children,
                    $nestedColumn  // pass the NestedColumn as the parent
                );

                // now update the children of the NestedColumn
                $nestedColumn->setChildren($children);

                $nestedMaxDefLevel = $currentMaxDefLevel;
                $nestedMaxRepLevel = $currentMaxRepLevel;

                foreach ($children as $child) {
                    $nestedMaxDefLevel = \max($nestedMaxDefLevel, $child->maxDefinitionsLevel());
                    $nestedMaxRepLevel = \max($nestedMaxRepLevel, $child->maxRepetitionsLevel());
                }

                $columns[] = $nestedColumn;  // use the updated NestedColumn
            } else {
                $columns[] = $root;
            }

            $processed_children++;
        }

        return $columns;
    }
}
