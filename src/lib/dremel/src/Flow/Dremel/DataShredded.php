<?php

declare(strict_types=1);

namespace Flow\Dremel;

use Flow\Dremel\Exception\InvalidArgumentException;

final class DataShredded
{
    public function __construct(
        public readonly array $repetitionLevels = [],
        public readonly array $definitionLevels = [],
        public readonly array $values = [],
    ) {
        if (\count($this->repetitionLevels) !== \count($this->definitionLevels)) {
            throw new InvalidArgumentException('Repetition levels and definition levels must have the same length');
        }
    }

    public function merge(self $dataShredded) : self
    {
        return new self(
            \array_merge($this->repetitionLevels, $dataShredded->repetitionLevels),
            \array_merge($this->definitionLevels, $dataShredded->definitionLevels),
            \array_merge($this->values, $dataShredded->values),
        );
    }

    public function size() : int
    {
        return \count($this->definitionLevels);
    }
}
