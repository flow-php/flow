<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\{Repetition, Repetitions};

final class DefinitionToValue
{
    public function __invoke(Repetitions $repetitions, int $definitionLevel, mixed $value) : mixed
    {
        if ($value === null && $definitionLevel === $repetitions->maxDefinitionLevel()) {
            throw new InvalidArgumentException('Value cannot be null for level "' . $definitionLevel . '" and max definition level "' . $repetitions->maxDefinitionLevel() . '"');
        }

        if ($value !== null && $definitionLevel < $repetitions->maxDefinitionLevel()) {
            throw new InvalidArgumentException('Value cannot be not null for level "' . $definitionLevel . '" and max definition level "' . $repetitions->maxDefinitionLevel() . '"');
        }

        if ($definitionLevel > $repetitions->maxDefinitionLevel()) {
            throw new InvalidArgumentException('Given definition level "' . $definitionLevel . '"  is greater than max level, "' . $repetitions->maxDefinitionLevel() . '"');
        }

        if ($value === null) {
            $value = new NullLevel($definitionLevel);
        }

        if ($repetitions->count() === 1) {
            if ($value instanceof NullLevel && $repetitions->first()->isRequired()) {
                throw new InvalidArgumentException('Value cannot be null for required field');
            }

            return $value;
        }

        if ($definitionLevel === $repetitions->maxDefinitionLevel()) {
            $node = $value;

            if ($value instanceof NullLevel && $repetitions->last()->isRequired()) {
                throw new InvalidArgumentException('Value cannot be null for required field');
            }

            foreach ($repetitions->toArray() as $repetition) {
                if ($repetition->isRepeated()) {
                    $node = [$node];
                }
            }

            return $node;
        }

        $repetitionsBranch = $repetitions->left($definitionLevel + 1);

        $partialValue = match ($repetitionsBranch->last()) {
            Repetition::REQUIRED => throw new InvalidArgumentException('Required field cannot be null'),
            Repetition::OPTIONAL => new NullLevel($definitionLevel),
            Repetition::REPEATED => [],
        };

        foreach ($repetitionsBranch->toArray() as $repetition) {

            if ($repetition->isRepeated()) {
                $partialValue = [$partialValue];
            }
        }

        if ($repetitionsBranch->last()->isRepeated()) {
            return $partialValue[0];
        }

        return $partialValue;
    }
}
