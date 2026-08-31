<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class GeneratorExtractor implements Extractor
{
    private ?Schema $schema = null;

    /**
     * @param \Generator<Rows> $rows
     */
    public function __construct(
        private Generator $rows,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rows as $row) {
            // @mago-ignore analysis:impossible-condition
            if (!$row instanceof Rows) {
                // @mago-ignore analysis:invalid-class-string-expression,invalid-operand
                throw new InvalidArgumentException('Passed generator can contain only Rows class instances, given: '
                . $row::class);
            }

            if ($this->schema !== null) {
                $row = array_to_rows($row->toArray(), $context->hydrator(), $this->schema);
            }

            $signal = yield $row;

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

        throw SchemaNotDerivableException::pipeline(self::class);
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
