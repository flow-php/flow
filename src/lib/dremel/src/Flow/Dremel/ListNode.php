<?php

declare(strict_types=1);

namespace Flow\Dremel;

final class ListNode implements Node
{
    private int $maxRepetitionLevel;

    private ?int $previousLevel;

    private array $value;

    public function __construct(int $maxRepetitionLevel)
    {
        $this->maxRepetitionLevel = $maxRepetitionLevel;
        $this->value = $this->initializeList($maxRepetitionLevel);
        $this->previousLevel = null;
    }

    public function push(mixed $value, int $level) : self
    {
        if ($level > $this->maxRepetitionLevel) {
            throw new \RuntimeException('Invalid level, max repetition level is ' . $this->maxRepetitionLevel . ' but ' . $level . ' was given');
        }

        if ($level === 0) {
            throw new \RuntimeException('Invalid level, level must be greater than 0');
        }

        $this->pushToLevel($this->value, $value, $level, $level);

        $this->previousLevel = $level;

        return $this;
    }

    public function value() : array
    {
        return $this->value;
    }

    private function initializeList(int $level) : array
    {
        if ($level === 1) {
            return [];
        }

        return [$this->initializeList($level - 1)];
    }

    private function initializeListWithValue(int $level, mixed $value) : array
    {
        if ($level === 1) {
            return [$value];
        }

        return [$this->initializeListWithValue($level - 1, $value)];
    }

    private function pushToLevel(array &$array, mixed $value, int $level, int $nextLevel) : void
    {
        if ($nextLevel === 1) {
            if ($this->previousLevel === null) {
                $array[] = $value;

                return;
            }

            if ($this->previousLevel > $level) {
                $array[] = $this->initializeListWithValue($this->maxRepetitionLevel - $level, $value);

                return;
            }

            if ($level === $this->maxRepetitionLevel) {
                $array[] = $value;

                return;
            }

            if ($level === $this->previousLevel) {
                $array[] = $this->initializeListWithValue($this->maxRepetitionLevel - $this->previousLevel, $value);

                return;
            }

            if ($this->previousLevel < $level) {
                $array[] = $this->initializeListWithValue($this->maxRepetitionLevel - $level, $value);

                return;
            }

            return;
        }

        $this->pushToLevel($array[\count($array) - 1], $value, $level, $nextLevel - 1);
    }
}
