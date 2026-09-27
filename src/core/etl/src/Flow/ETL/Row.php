<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Reference;
use Flow\Types\Type\TypedValueFormatter;
use Flow\Types\Value\Json;

use function array_values;

final readonly class Row
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        public Rows $rows,
        public int $index,
    ) {
        if ($index < 0 || $index >= $rows->count()) {
            throw InvalidArgumentException::because(
                'Row %d does not exist in a batch of %d rows',
                $index,
                $rows->count(),
            );
        }
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function get(string|Reference $reference): mixed
    {
        // @mago-ignore analysis:mixed-return-statement
        return $this->rows
            ->column($reference instanceof Reference ? $reference->base() : $reference)
            ->value($this->index);
    }

    public function has(string|Reference ...$references): bool
    {
        foreach ($references as $reference) {
            if (
                $this->rows->schema()->findDefinition($reference instanceof Reference ? $reference->base() : $reference)
                === null
            ) {
                return false;
            }
        }

        return true;
    }

    public function hash(Schema $schema, Algorithm $algorithm = new NativePHPHash()): string
    {
        $formatter = new TypedValueFormatter();
        $string = '';

        foreach ($schema->sort()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $string .= $name . $formatter->format($definition->type(), $this->has($name) ? $this->get($name) : null);
        }

        return $algorithm->hash($string);
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_values($this->rows->schema()->references()->names());
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $withKeys = true): array
    {
        $data = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values() as $name => $value) {
            if ($value instanceof Json) {
                $value = $value->toArray();
            }

            $withKeys ? ($data[$name] = $value) : ($data[] = $value);
        }

        return $data;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function values(): array
    {
        return $this->rows->values($this->index);
    }
}
