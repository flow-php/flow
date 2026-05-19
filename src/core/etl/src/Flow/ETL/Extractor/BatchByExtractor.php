<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function count;

final readonly class BatchByExtractor implements Extractor, OverridingExtractor
{
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
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    /**
     * @return \Generator<int, Rows, mixed, mixed>
     */
    public function extract(FlowContext $context): Generator
    {
        $buffer = [];
        $currentGroupValue = null;

        foreach ($this->extractor->extract($context) as $rows) {
            foreach ($rows->all() as $row) {
                $groupValue = $row->valueOf($this->column);

                if ($currentGroupValue === null) {
                    $currentGroupValue = $groupValue;
                } elseif ($currentGroupValue !== $groupValue) {
                    if ($this->minSize === null || count($buffer) >= $this->minSize) {
                        $signal = yield new Rows(...$buffer);

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
            yield new Rows(...$buffer);
        }
    }

    public function extractors(): array
    {
        return [$this->extractor];
    }
}
