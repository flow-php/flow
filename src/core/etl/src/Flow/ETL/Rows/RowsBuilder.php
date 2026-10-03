<?php

declare(strict_types=1);

namespace Flow\ETL\Rows;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ColumnBuilder;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\ColumnName;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

use function array_diff_key;
use function array_key_exists;
use function array_values;
use function count;
use function Flow\Types\DSL\type_array;
use function implode;
use function is_array;
use function is_int;
use function sprintf;

final class RowsBuilder
{
    /**
     * @var array<string, ColumnBuilder>
     */
    private array $builders = [];

    private int $count = 0;

    /**
     * @var list<string>
     */
    private array $names = [];

    /**
     * @var list<Definition<mixed>>
     */
    private array $definitions = [];

    private ?Rows $source = null;

    /**
     * @var list<?Column> per target position: the source column of the same name, null when the source lacks it
     */
    private array $columns = [];

    /**
     * @var list<bool> per target position: the source column shares the target definition
     */
    private array $shared = [];

    /**
     * @var array<int, Column> per target position, filled only when every target column is copied from the source
     */
    private array $copies = [];

    private ?string $unexpected = null;

    public function __construct(
        private readonly Schema $schema,
        Backend $backend,
    ) {
        foreach ($schema->definitions() as $name => $definition) {
            $this->builders[$name] = $backend->builder($definition);
            $this->names[] = $definition->entry()->name();
            $this->definitions[] = $definition;
        }
    }

    /**
     * @param array<array-key, mixed> $row name-keyed; a key the schema lacks is ignored
     *
     * @throws SchemaMismatchException
     */
    public function append(array $row): self
    {
        return $this->appendRows([$row]);
    }

    /**
     * A column sharing its definition with the target is copied physically; any other is refused unless its value
     * matches the target definition as it is - views are never cast.
     *
     * @throws SchemaMismatchException
     */
    public function appendFrom(Rows $rows, int $i): self
    {
        if ($rows !== $this->source) {
            $sources = $rows->schema()->definitions();
            $present = 0;
            $copies = [];
            $this->unexpected = null;

            foreach ($this->names as $position => $name) {
                if (!array_key_exists($name, $sources)) {
                    $this->columns[$position] = null;
                    $this->shared[$position] = false;

                    continue;
                }

                $present++;
                $column = $rows->column($name);
                $this->columns[$position] = $column;
                $this->shared[$position] = $sources[$name]->isSame($this->definitions[$position]);

                if ($this->shared[$position]) {
                    $copies[$position] = $column;
                }
            }

            if (count($sources) > $present) {
                foreach ($sources as $definition) {
                    if (!array_key_exists($definition->entry()->name(), $this->builders)) {
                        $this->unexpected = $definition->entry()->name();

                        break;
                    }
                }
            }

            $this->copies = $this->unexpected === null && count($copies) === count($this->names) ? $copies : [];
            $this->source = $rows;
        }

        if ($this->copies !== []) {
            foreach ($this->copies as $position => $column) {
                $this->builders[$this->names[$position]]->appendFrom($column, $i);
            }

            $this->count++;

            return $this;
        }

        $values = [];

        foreach ($this->definitions as $position => $definition) {
            $column = $this->columns[$position];

            if ($column === null) {
                if (!$definition->isNullable()) {
                    throw new SchemaMismatchException(
                        $this->count,
                        ColumnMismatchException::missingColumn($definition),
                    );
                }

                $values[$position] = null;

                continue;
            }

            if ($this->shared[$position]) {
                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            $value = $column->value($i);

            if ($value === null ? !$definition->isNullable() : !$definition->matches($value)) {
                throw new SchemaMismatchException(
                    $this->count,
                    ColumnMismatchException::valueDoesNotMatch($definition, $value),
                );
            }

            $values[$position] = $value;
        }

        if ($this->unexpected !== null) {
            throw new SchemaMismatchException(
                $this->count,
                ColumnMismatchException::unexpectedColumn($this->unexpected),
            );
        }

        foreach ($this->names as $position => $name) {
            $column = $this->columns[$position];

            if ($column === null || !$this->shared[$position]) {
                $this->builders[$name]->append($values[$position] ?? null);

                continue;
            }

            $this->builders[$name]->appendFrom($column, $i);
        }

        $this->count++;

        return $this;
    }

    /**
     * Records projected onto the schema: a key it does not declare is dropped, an int key is kept under its positional
     * name (ColumnName) when the schema declares that name.
     *
     * @param array<array<array-key, mixed>> $rows
     *
     * @throws SchemaMismatchException
     */
    public function appendProjected(array $rows): self
    {
        $definitions = $this->schema->definitions();
        $columnName = new ColumnName();
        $maps = [];

        foreach ($rows as $row) {
            $map = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $key => $value) {
                if (array_key_exists($key, $definitions)) {
                    $map[$key] = $value;

                    continue;
                }

                if (is_int($key) && $this->schema->findDefinition($name = $columnName->of($key)) !== null) {
                    $map[$name] = $value;
                }
            }

            $maps[] = $map;
        }

        return $this->appendRows($maps);
    }

    /**
     * One record or a list of them. A key the schema does not declare is refused at its row; a numeric-string key
     * PHP turned into an int keeps its declared name, any other int key is named by its position (ColumnName).
     *
     * @param array<array-key, mixed> $data
     *
     * @throws SchemaMismatchException
     */
    public function appendRecords(array $data): self
    {
        $isRows = true;

        // @mago-ignore analysis:mixed-assignment
        foreach ($data as $v) {
            if (!is_array($v)) {
                $isRows = false;

                break;
            }
        }

        $definitions = $this->schema->definitions();
        $columnName = new ColumnName();
        $maps = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (array_values($isRows ? $data : [$data]) as $index => $row) {
            $row = type_array()->assert($row);

            if (array_diff_key($row, $definitions) === []) {
                $maps[] = $row;

                continue;
            }

            $map = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $key => $value) {
                // PHP gives back a numeric-string column name as an int key, which the positional rule
                // would rename to eNN. A declared schema naming that column settles which one it is.
                $declared = $this->schema->findDefinition((string) $key);
                $name = $declared === null ? $columnName->of($key) : (string) $key;

                if ($declared === null && $this->schema->findDefinition($name) === null) {
                    throw new SchemaMismatchException(
                        $this->count + $index,
                        ColumnMismatchException::unexpectedColumn($name),
                    );
                }

                $map[$name] = $value;
            }

            $maps[] = $map;
        }

        return $this->appendRows($maps);
    }

    /**
     * @param list<array<array-key, mixed>> $rows name-keyed; a declared key a row lacks is null, or a missing column
     *                                            under NOT NULL; a key the schema lacks is ignored
     *
     * @throws SchemaMismatchException
     */
    public function appendRows(array $rows): self
    {
        if ($rows === []) {
            return $this;
        }

        $refusal = null;
        $absence = null;

        foreach ($this->definitions as $position => $definition) {
            $name = $this->names[$position];
            /** @var list<mixed> $values */
            $values = array_column($rows, $name);
            $present = null;

            if (count($values) !== count($rows)) {
                $values = [];
                $absent = null;

                foreach ($rows as $index => $row) {
                    if (array_key_exists($name, $row)) {
                        $values[] = $row[$name];

                        continue;
                    }

                    if (!$definition->isNullable()) {
                        $absent ??= $index;
                    }

                    $values[] = null;
                }

                // an absence under NOT NULL is refused below; the values the rows do carry are still checked, because
                // a refused value wins over an absence
                if ($absent !== null) {
                    if ($absence === null || ($this->count + $absent) < $absence->rowIndex) {
                        $absence = new SchemaMismatchException(
                            $this->count + $absent,
                            ColumnMismatchException::missingColumn($definition),
                        );
                    }

                    $present = array_keys(array_filter($rows, static fn(array $row): bool => array_key_exists(
                        $name,
                        $row,
                    )));
                    $values = array_map(static fn(int $index): mixed => $rows[$index][$name], $present);
                }
            }

            try {
                $this->builders[$name]->appendMany($values);
            } catch (SchemaMismatchException $e) {
                $row = $this->count + ($present === null ? $e->rowIndex : $present[$e->rowIndex]);

                if ($refusal === null || $row < $refusal->rowIndex) {
                    $refusal = new SchemaMismatchException($row, $e->cause);
                }
            }
        }

        if ($refusal !== null) {
            throw $refusal;
        }

        if ($absence !== null) {
            throw $absence;
        }

        $this->count += count($rows);

        return $this;
    }

    /**
     * @param list<int> $indices
     */
    public function appendTake(Rows $rows, array $indices): self
    {
        foreach ($this->schema->definitions() as $definition) {
            $name = $definition->entry()->name();
            $this->builders[$name]->appendTake($rows->column($name), $indices);
        }

        $this->count += count($indices);

        return $this;
    }

    public function column(string $name): ColumnBuilder
    {
        return (
            $this->builders[$name] ?? throw new InvalidArgumentException(sprintf(
                'RowsBuilder has no column "%s"',
                $name,
            ))
        );
    }

    public function count(): int
    {
        foreach ($this->builders as $builder) {
            return $builder->count();
        }

        return $this->count;
    }

    public function finish(): Rows
    {
        $columns = [];
        $counts = [];

        foreach ($this->builders as $name => $builder) {
            $columns[$name] = $builder->finish();
            $counts[$columns[$name]->count()][] = $name;
        }

        if (count($counts) > 1) {
            $ragged = [];

            foreach ($counts as $count => $names) {
                $ragged[] = sprintf('%d rows in [%s]', $count, implode(', ', $names));
            }

            throw new InvalidArgumentException('Ragged columns: ' . implode(', ', $ragged));
        }

        return Rows::fromColumns($this->schema, $columns, $this->count());
    }
}
