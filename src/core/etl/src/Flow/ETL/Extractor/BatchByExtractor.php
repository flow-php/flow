<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;

final class BatchByExtractor implements Extractor, OverridingExtractor
{
    private ?Schema $schema = null;

    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Extractor $extractor,
        private Reference $column,
        private ?int $minSize = null,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison,redundant-logical-operation
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        // pinned from the declaration or the first child batch, then every later batch is matched to
        // it - a buffer spans child batches, so its rows must all answer to one schema before trusted()
        $schema = $this->schema;

        $buffer = [];
        $currentGroupValue = null;

        foreach ($this->extractor->extract($context) as $rows) {
            $schema ??= $rows->schema();
            $rows = $rows->matchTo($schema);

            foreach ($rows->all() as $row) {
                $groupValue = $row->get($this->column);

                if ($currentGroupValue === null) {
                    $currentGroupValue = $groupValue;
                } elseif ($currentGroupValue !== $groupValue) {
                    if ($this->minSize === null || count($buffer) >= $this->minSize) {
                        $signal = yield Rows::trusted($schema, $buffer);

                        if ($signal === Signal::STOP) {
                            return;
                        }

                        $buffer = [];
                    }

                    $currentGroupValue = $groupValue;
                }

                $buffer[] = $row;
            }
        }

        if (count($buffer) > 0) {
            yield Rows::trusted($schema ?? $this->schema(), $buffer);
        }
    }

    public function extractors(): array
    {
        return [$this->extractor];
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        return $this->extractor->schema();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
