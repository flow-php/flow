<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\Types\Type\TypeDetector;

use function count;
use function Flow\ETL\DSL\definition_from_type;

/**
 * Values carry no declared type, so the batch schema is folded from the values themselves.
 */
final readonly class InferredBatch
{
    public function __construct(
        private TypeDetector $typeDetector = new TypeDetector(),
    ) {}

    /**
     * @param list<array<array-key, mixed>> $batch
     */
    public function of(array $batch): Rows
    {
        $schema = new Schema();

        foreach ($batch as $values) {
            $definitions = [];

            /** @var mixed $value */
            foreach ($values as $name => $value) {
                // PHP hands a numeric column name back as an INT key - a pivot names its columns by their own values
                $definitions[] = definition_from_type(
                    (string) $name,
                    $this->typeDetector->detectType($value),
                    $value === null,
                );
            }

            $schema = $schema->merge(new Schema(...$definitions));
        }

        $backend = new DefaultBackend();

        // one row's schema is typed from its own values, so every value already fits: one constant per column spares
        // the builders a single-row batch does not need
        if (count($batch) === 1) {
            $columns = [];

            foreach ($schema->definitions() as $name => $definition) {
                $columns[$name] = $backend->constant($definition, $batch[0][$name], 1);
            }

            return Rows::fromColumns($schema, $columns, 1);
        }

        // the fold widens a column typed from one row's value to cover every row's, so the builder casts each value
        // to the schema this method just derived
        return (new RowsBuilder($schema, $backend))
            ->appendRows($batch)
            ->finish();
    }
}
