<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\SimilarNames;
use Flow\Types\Type\TypedValueFormatter;
use Flow\Types\Value\Json;

use function array_key_exists;
use function array_keys;
use function array_map;
use function array_values;
use function count;
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
     * Rewrites the storage to satisfy $schema: columns take the Schema's order and a declared
     * nullable column the row omits is padded with null. The row does not know its position in a
     * batch - Rows places the violation with SchemaMismatchException.
     *
     * @throws ColumnMismatchException
     */
    public function matchTo(Schema $schema): self
    {
        return $this->conform($schema, checkValues: true);
    }

    /**
     * matchTo() without its value check: the order, padding, missing, unknown and null rules all hold, but a non-null
     * value is not validated against its type - the caller produced it by casting to, or decoding from, that type.
     *
     * @throws ColumnMismatchException
     */
    public function conformTo(Schema $schema): self
    {
        return $this->conform($schema, checkValues: false);
    }

    /**
     * Drops the columns $schema does not declare and carries its order into the row. Unlike
     * matchTo() it validates nothing - the caller is changing the shape, not checking it.
     */
    public function project(Schema $schema): self
    {
        $projected = [];

        foreach ($schema->definitions() as $name => $_) {
            if (array_key_exists($name, $this->values)) {
                $projected[$name] = $this->values[$name];
            }
        }

        return new self($projected);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function get(string|Reference $reference): mixed
    {
        $name = $reference instanceof Reference ? $reference->base() : $reference;

        if (!array_key_exists($name, $this->values)) {
            $suggestions = (new SimilarNames())->closestTo(
                $name,
                array_values(array_map(
                    static fn(int|string $column): string => (string) $column,
                    array_keys($this->values),
                )),
            );

            throw new InvalidArgumentException(
                $suggestions === []
                    ? "Column \"{$name}\" does not exist."
                    : "Column \"{$name}\" does not exist. Did you mean one of the following? [\""
                    . implode('", "', $suggestions)
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

    /**
     * @throws ColumnMismatchException
     */
    private function conform(Schema $schema, bool $checkValues): self
    {
        $definitions = $schema->definitions();
        $matched = [];
        $taken = 0;

        foreach ($definitions as $name => $definition) {
            if (!array_key_exists($name, $this->values)) {
                if (!$definition->isNullable()) {
                    throw ColumnMismatchException::missingColumn($definition);
                }

                $matched[$name] = null;

                continue;
            }

            $taken++;

            if (
                $checkValues
                    ? !$definition->matches($this->values[$name])
                    : $this->values[$name] === null && !$definition->isNullable()
            ) {
                throw ColumnMismatchException::valueDoesNotMatch($definition, $this->values[$name]);
            }

            $matched[$name] = $this->values[$name];
        }

        if ($taken !== count($this->values)) {
            foreach ($this->values as $name => $_) {
                if (!array_key_exists($name, $definitions)) {
                    throw ColumnMismatchException::unexpectedColumn($name);
                }
            }
        }

        return new self($matched);
    }
}
