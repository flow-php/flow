<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\ColumnName;
use Flow\ETL\Schema;
use Flow\Types\Type;
use Flow\Types\Type\TypeDetector;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\schema;
use function is_array;

final class ArrayExtractor implements Extractor
{
    private ?Schema $schema = null;

    /**
     * @param iterable<array<mixed>> $dataset
     */
    public function __construct(
        private readonly iterable $dataset,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        // Resolved before the first row, so every row that follows is hydrated against the same
        // shape. A non-rewindable dataset cannot be described, and schema() says so rather than
        // quietly typing each row on its own.
        $schema = $this->schema();

        foreach ($this->dataset as $row) {
            $signal = yield array_to_rows([$row], $schema, $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        if (!is_array($this->dataset)) {
            throw SchemaNotDerivableException::nonRewindable(self::class);
        }

        $schema = new Schema();

        foreach ($this->dataset as $row) {
            $definitions = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $key => $value) {
                $definitions[] = definition_from_type(
                    (new ColumnName())->of($key),
                    (new TypeDetector())->detectType($value),
                );
            }

            $schema = $schema->merge(schema(...$definitions));
        }

        return $this->schema = $schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
