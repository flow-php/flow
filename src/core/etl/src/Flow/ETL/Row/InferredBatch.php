<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type\TypeDetector;

use function array_key_exists;
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
     * @param list<RawRowValues> $batch
     */
    public function of(array $batch): Rows
    {
        $schema = new Schema();

        foreach ($batch as $rowValues) {
            $definitions = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                // RawRowValues::$values is declared array<string, mixed>, but PHP hands a numeric
                // column name back as an INT key - a pivot names its columns by their own values.
                // definition_from_type() takes Reference|string, so the cast is not redundant at runtime.
                // @mago-ignore analysis:redundant-cast
                $name = (string) $name;
                $definition = definition_from_type($name, $this->typeDetector->detectType($value), $value === null);

                if (array_key_exists($name, $rowValues->metadata)) {
                    $definition = $definition->setMetadata($rowValues->metadata[$name]);
                }

                $definitions[] = $definition;
            }

            $schema = $schema->merge(new Schema(...$definitions));
        }

        $rows = [];

        // the fold widens a column typed from one row's value to cover every row's - so a value the
        // widened definition no longer accepts follows the schema this method just derived
        foreach ($batch as $rowValues) {
            $values = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                // see above - an int key here would miss the column the schema declares as "5"
                // @mago-ignore analysis:redundant-cast
                $name = (string) $name;
                $definition = $schema->get($name);

                $values[$name] = $definition->matches($value) ? $value : $definition->type()->cast($value);
            }

            $rows[] = new Row($values);
        }

        return new Rows($schema, ...$rows);
    }
}
