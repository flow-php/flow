<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\SequenceGenerator\SequenceGenerator;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\ColumnName;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type\TypeDetector;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\schema;

final class SequenceExtractor implements Extractor
{
    private ?Schema $schema = null;

    public function __construct(
        private readonly SequenceGenerator $generator,
        private readonly string $entryName = 'entry',
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        /** @var mixed $item */
        foreach ($this->generator->generate() as $item) {
            $signal = yield array_to_rows([[$this->entryName => $item]], $context->hydrator(), [], $schema);

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

        foreach ($this->generator->generate() as $row) {
            $definitions = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ([$this->entryName => $row] as $key => $value) {
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
