<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row\ColumnName;
use Flow\ETL\Schema;
use Flow\Types\Type\TypeDetector;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\schema;

final class MemoryExtractor implements Extractor
{
    private ?Schema $schema = null;

    public function __construct(
        private readonly Memory $memory,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        foreach ($this->memory->dump() as $row) {
            $signal = yield array_to_rows([$row], $context->hydrator(), $schema);

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

        $schema = new Schema();

        foreach ($this->memory->dump() as $row) {
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

        return $schema;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
