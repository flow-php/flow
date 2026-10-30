<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function max;

/**
 * One unit's fold, done: the column types in first-seen order and the rows they were observed over.
 */
final readonly class SniffedColumns
{
    /**
     * @param array<array-key, Type<mixed>> $types first-seen order: header names first, then names met only in rows
     * @param int<0, max> $rows
     */
    public function __construct(
        public array $types,
        public int $rows,
        private TypeWidener $widener = new TypeWidener(),
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($rows < 0) {
            throw new InvalidArgumentException('Sniffed rows must not be negative');
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $types = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (type_array()->assert($data['columns'] ?? null) as $column) {
            $column = type_array()->assert($column);
            /** @var array<string, mixed> $type */
            $type = type_array()->assert($column['type'] ?? null);
            $types[type_string()->assert($column['name'] ?? null)] = TypeFactory::fromArray($type);
        }

        return new self($types, max(0, type_integer()->assert($data['rows'] ?? null)));
    }

    /**
     * Merge left-to-right in listing order: name order follows first-seen, so a different bracketing of the name
     * sequence changes the definition order (never the types).
     */
    public function merge(self $other, bool $unionByName): self
    {
        $types = $this->types;

        foreach ($other->types as $name => $type) {
            if (!array_key_exists($name, $types)) {
                if (!$unionByName) {
                    continue;
                }

                $types[$name] = $type;

                continue;
            }

            $types[$name] = $this->widener->widen($types[$name], $type);
        }

        return new self($types, $this->rows + $other->rows, $this->widener);
    }

    /**
     * A numeric column name is an int key here and a string in the JSON: the name travels as a string either way.
     *
     * @return array{columns: list<array{name: string, type: array<string, mixed>}>, rows: int}
     */
    public function normalize(): array
    {
        $columns = [];

        foreach ($this->types as $name => $type) {
            $columns[] = ['name' => (string) $name, 'type' => $type->normalize()];
        }

        return ['columns' => $columns, 'rows' => $this->rows];
    }

    public function schema(TypeFloor $floor): Schema
    {
        $definitions = [];

        foreach ($this->types as $name => $type) {
            $definitions[] = Definitions::of((string) $name, $floor->floor($type), nullable: true);
        }

        return new Schema(...$definitions);
    }
}
