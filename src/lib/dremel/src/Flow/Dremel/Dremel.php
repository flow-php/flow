<?php

declare(strict_types=1);

namespace Flow\Dremel;

final class Dremel
{
    public function __construct()
    {
    }

    public function assemble(DataShredded $data, array $repetitions, int $maxDefinitionLevel) : DataAssembled
    {
        $totalIterations = $data->size();
        $valueIndex = 0;

        $rows = [];

        for ($iteration = 0; $iteration < $totalIterations; $iteration++) {
            $definitionLevel = $data->definitionLevels[$iteration];
            $repetitionLevel = $data->repetitionLevels[$iteration];

            $this->buildValue($element, $repetitions, $maxDefinitionLevel, $definitionLevel, $data->values, $valueIndex);

            $this->updateStack($repetitionLevel, $rows, $element);
            unset($element);
        }

        return new DataAssembled($rows, $data);
    }

    /**
     * @param array<mixed> $data
     */
    public function shred(array $data, array $repetitions) : DataShredded
    {
        $repetitionLevels = [];
        $definitionLevels = [];
        $values = [];

        $this->recurseShred($data, $repetitionLevels, $definitionLevels, $values, $repetitions, 0, 0);

        return new DataShredded(
            $repetitionLevels,
            $definitionLevels,
            $values,
        );
    }

    private function buildValue(?array &$element, array $repetitions, int $maxDefinitionLevel, int $definitionLevel, array $values, int &$valueIndex, int $level = 0) : void
    {
        $repetition = array_shift($repetitions);

        if ($level === $maxDefinitionLevel) {
            $element = $values[$valueIndex];
            $valueIndex++;

            return;
        }

        if ($level === $definitionLevel) {
            if ($repetition === 'REPEATED') {
                $element = [];

                return;
            }

            if ($repetition === 'OPTIONAL') {
                $element = null;

                return;
            }
        }

        if ($repetition === 'REQUIRED') {
            $this->buildValue($element, $repetitions, $maxDefinitionLevel, $definitionLevel, $values, $valueIndex, $level);

            return;
        }

        if ($repetition === 'REPEATED') {
            $element = [];
            $this->buildValue($element[], $repetitions, $maxDefinitionLevel, $definitionLevel, $values, $valueIndex, $level + 1);

            return;
        }

        if ($repetition === 'OPTIONAL') {
            $this->buildValue($element, $repetitions, $maxDefinitionLevel, $definitionLevel, $values, $valueIndex, $level + 1);

            return;
        }
    }

    private function recurseShred(
        array $data,
        array &$repetitionLevels,
        array &$definitionLevels,
        array &$values,
        array $repetitions,
        int $currentRepetitionLevel,
        int $currentDefinitionLevel,
        int $level = 0,
    ) : void {

        foreach ($data as $rowIndex => $element) {
            if ($element === null) {
                $definitionLevels[] = $currentDefinitionLevel;

                if (\count($repetitionLevels) === 0 || $rowIndex === 0) {
                    $repetitionLevels[] = $currentRepetitionLevel;
                } else {
                    $repetitionLevels[] = $currentRepetitionLevel + $level;
                }

                // dj([
                //    'position' => '$element === null',
                //    'row_index' => $rowIndex,
                //    'level' => $level,
                //    'element' => $element,
                //    'values' => $values,
                //    'repetition_levels' => $repetitionLevels,
                //    'definition_levels' => $definitionLevels,
                //    'repetition' => $repetitions[$currentDefinitionLevel],
                //    'repetitions' => $repetitions,
                //    'current_repetitionLevel' => $currentRepetitionLevel,
                //    'current_definitionLevel' => $currentDefinitionLevel,
                // ]);

                continue;
            }

            if (\is_array($element)) {
                if (!\count($element)) {
                    $definitionLevels[] = $repetitions[$currentDefinitionLevel] !== 'REQUIRED' ? $currentDefinitionLevel + 1 : $currentDefinitionLevel;

                    if (\count($repetitionLevels) === 0 || $rowIndex === 0) {
                        $repetitionLevels[] = $currentRepetitionLevel;
                    } else {
                        $repetitionLevels[] = $currentRepetitionLevel + $level;
                    }

                    // dj([
                    //    'position' => 'is_array($element) && !count(element)',
                    //    'row_index' => $rowIndex,
                    //    'level' => $level,
                    //    'element' => $element,
                    //    'values' => $values,
                    //    'repetition_levels' => $repetitionLevels,
                    //    'definition_levels' => $definitionLevels,
                    //    'repetition' => $repetitions[$currentDefinitionLevel],
                    //    'repetitions' => $repetitions,
                    //    'current_repetitionLevel' => $currentRepetitionLevel,
                    //    'current_definitionLevel' => $currentDefinitionLevel,
                    // ]);

                    continue;
                }

                $this->recurseShred(
                    $element,
                    $repetitionLevels,
                    $definitionLevels,
                    $values,
                    $repetitions,
                    $currentRepetitionLevel,
                    $repetitions[$currentDefinitionLevel] !== 'REQUIRED' ? $currentDefinitionLevel + 2 : $currentDefinitionLevel + 1,
                    $level + 1
                );

                continue;
            }

            if ($element === null) {
                $definitionLevels[] = $repetitions[$currentDefinitionLevel] !== 'REQUIRED' ? $currentDefinitionLevel + 1 : $currentDefinitionLevel;
            } else {
                $definitionLevels[] = \array_reduce($repetitions, fn ($carry, $item) => $carry + ($item !== 'REQUIRED' ? 1 : 0), 0);
            }

            if (\count($repetitionLevels) === 0 || $rowIndex === 0) {
                $repetitionLevels[] = $currentRepetitionLevel;
            } else {
                $repetitionLevels[] = $currentRepetitionLevel + $level;
            }

            $values[] = $element;

            // dj([
            //    'position' => 'end of rows loop',
            //    'row_index' => $rowIndex,
            //    'level' => $level,
            //    'element' => $element,
            //    'values' => $values,
            //    'repetition_levels' => $repetitionLevels,
            //    'definition_levels' => $definitionLevels,
            //    'repetition' => $repetitions[$currentDefinitionLevel],
            //    'repetitions' => $repetitions,
            //    'current_repetitionLevel' => $currentRepetitionLevel,
            //    'current_definitionLevel' => $currentDefinitionLevel,
            // ]);
        }
    }

    private function updateStack(int $repetition, array &$stack, mixed $element) : void
    {
        // if repetition is 0, add element as a new entry in stack
        if ($repetition === 0) {
            $stack[] = $element;

            return;
        }

        // take the last element from the stack
        $stackLastElement = &$stack[count($stack) - 1];
        $currentElement = &$element;

        for ($i = 1; $i < $repetition; $i++) {
            $stackLastElement = &$stackLastElement[count($stackLastElement) - 1];
            $currentElement = &$currentElement[count($currentElement) - 1];
        }

        $stackLastElement = \array_merge($stackLastElement, $currentElement);
    }
}
