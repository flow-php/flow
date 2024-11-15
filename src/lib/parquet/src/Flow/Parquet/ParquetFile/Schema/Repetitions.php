<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;

final class Repetitions
{
    public function __construct(
        private readonly array $repetitions,
    ) {
    }

    public function __toString() : string
    {
        return \implode(',', \array_map(static fn (Repetition $r) => $r->name, $this->repetitions));
    }

    public function get(int $level) : Repetition
    {
        if (!\array_key_exists($level, $this->repetitions)) {
            throw new InvalidArgumentException(\sprintf('Repetition level %d does not exist: %s', $level, $this->__toString()));
        }

        return $this->repetitions[$level];
    }

    public function maxDefinitionLevel() : int
    {
        $maxDefinitionLevel = 0;

        foreach ($this->repetitions as $repetition) {
            if ($repetition !== Repetition::REQUIRED) {
                $maxDefinitionLevel++;
            }
        }

        return $maxDefinitionLevel;
    }

    public function maxRepetitionLevel() : int
    {
        $maxRepetitionLevel = 0;

        foreach ($this->repetitions as $repetition) {
            if ($repetition === Repetition::REPEATED) {
                $maxRepetitionLevel++;
            }
        }

        return $maxRepetitionLevel;
    }
}
