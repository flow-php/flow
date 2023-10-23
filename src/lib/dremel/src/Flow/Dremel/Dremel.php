<?php declare(strict_types=1);

namespace Flow\Dremel;

use function Flow\Parquet\array_flatten;
use Flow\Dremel\Exception\InvalidArgumentException;
use Flow\Dremel\Exception\RuntimeException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class Dremel
{
    public function __construct(private readonly LoggerInterface $logger = new NullLogger())
    {
    }

    /**
     * @param array<int> $repetitions
     * @param array<int> $definitions
     * @param array<mixed> $values
     *
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function assemble(array $repetitions, array $definitions, array $values) : \Generator
    {
        $this->assertInput($repetitions, $definitions);

        $output = [];
        $maxDefinitionLevel = \count($definitions) ? \max($definitions) : 0;
        $maxRepetitionLevel = \count($repetitions) ? \max($repetitions) : 0;

        $valueIndex = 0;

        if ($maxRepetitionLevel === 0) {
            foreach ($definitions as $definition) {
                if ($definition === 0) {
                    yield null;
                } elseif ($definition === $maxDefinitionLevel) {
                    yield $values[$valueIndex];
                    $valueIndex++;
                }
            }

            return;
        }

        $iteration = 0;
        $stack = new Stack();

        try {
            foreach ($definitions as $definitionIndex => $definition) {
                $repetition = $repetitions[$definitionIndex];

                if ($repetition === 0) {
                    if ($stack->size()) {
                        yield $stack->dropFlat();
                        $stack->clear();
                        $stack->push(new ListNode($maxRepetitionLevel));
                    } else {
                        $stack->push(new ListNode($maxRepetitionLevel));
                    }
                }

                if ($repetition === 0 && $definition === 0) {
                    yield null;
                    $stack->clear();
                } else {
                    if ($repetition <= $maxRepetitionLevel && $repetition > 0) {
                        /** @phpstan-ignore-next-line  */
                        $stack->last()->push(
                            $this->value($definition, $maxDefinitionLevel, $values, $valueIndex),
                            $repetition
                        );
                    } elseif ($repetition === 0) {
                        /** @phpstan-ignore-next-line  */
                        $stack->last()->push(
                            $this->value($definition, $maxDefinitionLevel, $values, $valueIndex),
                            $maxRepetitionLevel
                        );
                    }
                }

                $this->debugDecodeNested($iteration, $repetition, $definition, $maxDefinitionLevel, $maxRepetitionLevel, $stack, $output);
                $iteration++;
            }

            if ($stack->size()) {
                yield $stack->dropFlat();
                $stack->clear();
            }
        } catch (\Throwable $e) {
            $this->logger->error('[Dremel][Decode][Nested] error', [
                'exception' => $e,
                'iteration' => $iteration,
                'repetition' => $repetitions,
                'definition' => $definitions,
                'values' => $values,
                'max_definition_level' => $maxDefinitionLevel,
                'max_repetition_level' => $maxRepetitionLevel,
            ]);

            throw $e;
        }
    }

    /**
     * @param array<mixed> $data
     */
    public function shred(array $data, int $maxDefinitionLevel) : DataShredded
    {
        $definitions = [];
        $this->buildDefinitions($data, $definitions, $maxDefinitionLevel);

        return new DataShredded(
            $this->buildRepetitions($data),
            $definitions,
            array_flatten($data)
        );
    }

    private function arrayTypeToString(?array $inputArray) : string
    {
        if ($inputArray === null) {
            return 'null';
        }

        if (!\count($inputArray)) {
            return '';
        }

        $result = [];

        foreach ($inputArray as $item) {
            if (\is_array($item)) {
                $result[] = '[' . $this->arrayTypeToString($item) . ']';
            } else {
                if ($item === null) {
                    $result[] = 'null';
                } else {
                    $result[] = (string) $item;
                }
            }
        }

        return \implode(', ', $result);
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

    private function buildDefinitions(array $data, array &$definitions, int $maxDefinitionLevel) : void
    {
        foreach ($data as $key => $value) {
            if (\is_array($value)) {
                // Recursively call the function if the value is an array
                $this->buildDefinitions($value, $definitions, $maxDefinitionLevel);
            } else {
                if ($value === null) {
                    $definitions[] = $maxDefinitionLevel - 1;
                } else {
                    $definitions[] = $maxDefinitionLevel;
                }
            }
        }
    }

    private function buildRepetitions(array $data, int $currentLevel = 0, bool $newRow = true) : array
    {
        $output = [];

        foreach ($data as $item) {
            if (\is_array($item)) {
                $currentLevel++;

                $valueTypes = [];

                foreach ($item as $subItem) {
                    $valueTypes[] = \gettype($subItem);
                }

                if (\count(\array_unique($valueTypes)) !== 1) {
                    throw new RuntimeException('Invalid data structure, each row must be an array of arrays or scalars, got both, arrays and scalars. ' . \json_encode($item, \JSON_THROW_ON_ERROR));
                }

                $newRow = true;

                foreach ($item as $subItem) {
                    if (\is_array($subItem)) {
                        $output = \array_merge($output, $this->buildRepetitions($subItem, $currentLevel + 1, $newRow));
                    } else {
                        $output[] = $newRow ? 0 : $currentLevel;
                    }

                    $newRow = false;
                }
                $currentLevel--;
            } else {
                if (!\count($output)) {
                    $output[] = $newRow ? 0 : $currentLevel - 1;
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

    private function debugDecodeNested(int $iteration, ?int $repetition, int $definition, int $maxDefinitionLevel, int $maxRepetitionLevel, Stack $stack, array $output) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $stackDebug = '[' . $this->arrayTypeToString($stack->__debugInfo()) . ']';
        $outputDebug = '[' . $this->arrayTypeToString($output) . ']';

        $definitionDebug = $definition === $maxDefinitionLevel ? 'value' : 'null';

        $this->logger->debug('[Dremel][Decode][Nested] data structure', [
            'iteration' => $iteration,
            'repetition' => $repetition,
            'definition' => $definitionDebug,
            'max_definition_level' => $maxDefinitionLevel,
            'max_repetition_level' => $maxRepetitionLevel,
            'stack' => $stackDebug,
            'output' => $outputDebug,
        ]);
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
