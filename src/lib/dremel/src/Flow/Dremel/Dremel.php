<?php declare(strict_types=1);

namespace Flow\Dremel;

use function Flow\Parquet\array_flatten;
use Flow\Dremel\Exception\InvalidArgumentException;

final class Dremel
{
    public function __construct()
    {
    }

    /**
     * @param array<int> $repetitions
     * @param array<int> $definitions
     * @param array<mixed> $values
     *
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function assemble(array $repetitions, array $definitions, array $values) : array
    {
        $this->assertInput($repetitions, $definitions);

        $maxDefinitionLevel = \count($definitions) ? \max($definitions) : 0;
        $maxRepetitionLevel = \count($repetitions) ? \max($repetitions) : 0;

        $output = [];
        $valueIndex = 0;

        if ($maxRepetitionLevel === 0) {
            foreach ($definitions as $definition) {
                if ($definition === 0) {
                    $output[] = null;
                } elseif ($definition === $maxDefinitionLevel) {
                    $output[] = $values[$valueIndex] ?? null;
                    $valueIndex++;
                }
            }

            return $output;
        }

        $stack = new Stack();

        foreach ($definitions as $definitionIndex => $definition) {
            $repetition = $repetitions[$definitionIndex];

            if ($repetition === 0 && $definition !== 0) {
                $stack->push(new ListNode($maxRepetitionLevel));
            }

            if ($repetition === 0 && $definition === 0) {
                $stack->push(new NullNode());

                continue;
            }

            if ($definition + 1 >= $maxDefinitionLevel) {
                /** @phpstan-ignore-next-line  */
                $stack->last()->push(
                    $this->value($definition, $maxDefinitionLevel, $values, $valueIndex),
                    $repetition === 0 ? $maxRepetitionLevel : $repetition
                );
            }
        }

        return $stack->dropFlat();
    }

    /**
     * @param array<mixed> $data
     */
    public function shred(array $data, int $maxDefinitionLevel) : DataShredded
    {
        $definitions = [];
        $this->buildDefinitions($data, $definitions, $maxDefinitionLevel);
        $repetitions = $this->buildRepetitions($data);

        return new DataShredded(
            $repetitions,
            $definitions,
            \array_values(\array_filter(array_flatten($data), static fn ($item) => $item !== null))
        );
    }

    private function assertInput(array $repetitions, array $definitions) : void
    {
        if (\count($repetitions) !== 0) {
            if (\count(\array_unique([\count($repetitions), \count($definitions)])) !== 1) {
                throw new InvalidArgumentException('repetitions, definitions and values count must be exactly the same, repetitions: ' . \count($repetitions) . ', definitions: ' . \count($definitions));
            }
        }

        if (\count($repetitions)) {
            if ($repetitions[0] !== 0) {
                throw new InvalidArgumentException('Repetitions must start with zero, otherwise it probably means that your data was split into multiple pages in which case proper reconstruction of rows is impossible.');
            }
        }
    }

    private function buildDefinitions(array $data, array &$definitions, int $maxDefinitionLevel, int $level = 1) : void
    {
        $previousElementType = null;

        foreach ($data as $key => $value) {
            if (\is_array($value)) {
                if (!\count($value)) {
                    $definitions[] = $level;
                } else {
                    $this->buildDefinitions($value, $definitions, $maxDefinitionLevel, $level + 1);
                }
            } else {
                if ($value === null) {
                    if ($level === 1 || $previousElementType === 'array') {
                        $definitions[] = 0;
                    } else {
                        $definitions[] = $level;
                    }
                } else {
                    $definitions[] = $maxDefinitionLevel;
                }
            }

            $previousElementType = \gettype($value);
        }
    }

    private function buildRepetitions(array $data, int $currentLevel = 0, int $topIndex = 0) : array
    {
        $output = [];

        foreach ($data as $index => $item) {
            if (\is_array($item)) {

                if (!\count($item)) {
                    $output[] = 0;

                    continue;
                }

                $output = \array_merge($output, $this->buildRepetitions($item, $currentLevel + 1, $index));
            } else {
                if (!\count($output)) {
                    $output[] = $topIndex === 0 ? 0 : $currentLevel - 1;
                } else {
                    $output[] = $currentLevel;
                }
            }
        }

        if (!\count($output) || \max($output) === 0) {
            return [];
        }

        return $output;
    }

    private function value(int $definition, int $maxDefinitionLevel, array $values, int &$valueIndex) : mixed
    {
        if ($definition < $maxDefinitionLevel) {
            return null;
        }

        $value = $values[$valueIndex];
        $valueIndex++;

        return $value;
    }
}
