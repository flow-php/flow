<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;

final class Repetitions implements \Countable
{
    private ?int $maxDefinitionLevel = null;

    private ?int $maxRepetitionLevel = null;

    private array $repetitions;

    public function __construct(
        Repetition ...$repetitions,
    ) {
        if (!\count($repetitions)) {
            throw new InvalidArgumentException('Repetitions cannot be empty');
        }

        $this->repetitions = $repetitions;
    }

    public function __toString() : string
    {
        return \implode(',', \array_map(static fn (Repetition $r) => $r->name, $this->repetitions));
    }

    public function count() : int
    {
        return \count($this->repetitions);
    }

    public function first() : Repetition
    {
        return $this->repetitions[0];
    }

    public function get(int $index) : Repetition
    {
        if (!\array_key_exists($index, $this->repetitions)) {
            throw new InvalidArgumentException(\sprintf('Repetition index %d does not exist: %s', $index, $this->__toString()));
        }

        return $this->repetitions[$index];
    }

    public function last() : Repetition
    {
        return $this->repetitions[\count($this->repetitions) - 1];
    }

    public function left(int $index) : self
    {
        $repetitions = [];

        $currentLevel = 0;

        foreach ($this->repetitions as $repetition) {
            if (!$repetition->isRequired()) {
                $currentLevel++;
            }

            $repetitions[] = $repetition;

            if ($currentLevel === $index) {
                break;
            }
        }

        return new self(...$repetitions);
    }

    public function maxDefinitionLevel() : int
    {
        if ($this->maxDefinitionLevel !== null) {
            return $this->maxDefinitionLevel;
        }

        $maxDefinitionLevel = 0;

        foreach ($this->repetitions as $repetition) {
            if ($repetition !== Repetition::REQUIRED) {
                $maxDefinitionLevel++;
            }
        }

        $this->maxDefinitionLevel = $maxDefinitionLevel;

        return $this->maxDefinitionLevel;
    }

    public function maxRepetitionLevel() : int
    {
        if ($this->maxRepetitionLevel !== null) {
            return $this->maxRepetitionLevel;
        }

        $maxRepetitionLevel = 0;

        foreach ($this->repetitions as $repetition) {
            if ($repetition === Repetition::REPEATED) {
                $maxRepetitionLevel++;
            }
        }

        $this->maxRepetitionLevel = $maxRepetitionLevel;

        return $this->maxRepetitionLevel;
    }

    /**
     * @param array<Repetition> $repetitions
     */
    public function toArray() : array
    {
        return $this->repetitions;
    }
}
