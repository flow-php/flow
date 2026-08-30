<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Reference;
use Flow\Types\Type\TypedValueFormatter;
use Flow\Types\Value\Json;

use function array_key_exists;
use function array_keys;
use function implode;

final readonly class Row
{
    /**
     * @param array<string, mixed> $values storage keyed by name; the Schema defines column order (Rows::schema()->references())
     */
    public function __construct(
        private array $values,
    ) {}

    /**
     * @throws InvalidArgumentException
     *
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function get(string|Reference $reference): mixed
    {
        $name = $reference instanceof Reference ? $reference->base() : $reference;

        if (!array_key_exists($name, $this->values)) {
            throw new InvalidArgumentException(
                "Column \"{$name}\" does not exist. Did you mean one of the following? [\""
                . implode('", "', array_keys($this->values))
                . '"]',
            );
        }

        // @mago-ignore analysis:mixed-return-statement
        return $this->values[$name];
    }

    public function has(string|Reference ...$references): bool
    {
        foreach ($references as $reference) {
            if (!array_key_exists($reference instanceof Reference ? $reference->base() : $reference, $this->values)) {
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
            $string .= $name . $formatter->format($definition->type(), $this->values[$name] ?? null);
        }

        return $algorithm->hash($string);
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_keys($this->values);
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $withKeys = true): array
    {
        $data = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values as $name => $value) {
            if ($value instanceof Json) {
                $value = $value->toArray();
            }

            $withKeys ? ($data[$name] = $value) : ($data[] = $value);
        }

        return $data;
    }

    /**
     * @return array<string, mixed>
     */
    public function values(): array
    {
        return $this->values;
    }
}
