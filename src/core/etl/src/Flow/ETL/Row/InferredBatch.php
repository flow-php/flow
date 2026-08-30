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
        $rows = [];

        foreach ($batch as $rowValues) {
            $definitions = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                $definition = definition_from_type($name, $this->typeDetector->detectType($value), $value === null);

                if (array_key_exists($name, $rowValues->metadata)) {
                    $definition = $definition->setMetadata($rowValues->metadata[$name]);
                }

                $definitions[] = $definition;
            }

            $schema = $schema->merge(new Schema(...$definitions));
            $rows[] = new Row($rowValues->values);
        }

        return new Rows($schema, ...$rows);
    }
}
