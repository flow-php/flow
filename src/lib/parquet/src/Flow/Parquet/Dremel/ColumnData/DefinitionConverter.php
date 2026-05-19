<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\Repetitions;

use function array_key_exists;
use function is_array;

/**
 * Converts definition levels to actual values.
 * Initially this class was much simpler but in order to keep the performance and not to repeat the same operations
 * over and over again, we generate templates for each definition level.
 * It's a trade off between DX and performance but performance is more important here.
 */
final class DefinitionConverter
{
    /**
     * @var array<
     *     string,
     *     array{
     *         templates: array<int, array|NullLevel>,
     *         max_definition_level: int,
     *         repeated_count: int,
     *         is_flat: bool,
     *         is_first_required: bool,
     *       }
     *     >
     */
    private array $templates = [];

    public function toValue(Repetitions $repetitions, int $definitionLevel, mixed $value): mixed
    {
        if (!array_key_exists($repetitions->id, $this->templates)) {
            $this->generateTemplates($repetitions, $repetitions->maxDefinitionLevel());
        }

        $maxDefinitionLevel = $this->templates[$repetitions->id]['max_definition_level'];

        $this->validate($value, $definitionLevel, $maxDefinitionLevel);

        if ($value === null) {
            $value = new NullLevel($definitionLevel);
        }

        if ($this->templates[$repetitions->id]['is_flat'] === true) {
            if ($value instanceof NullLevel && $this->templates[$repetitions->id]['is_first_required']) {
                throw new InvalidArgumentException('Value cannot be null for required field');
            }

            return $value;
        }

        if ($definitionLevel === $maxDefinitionLevel) {
            if ($this->templates[$repetitions->id]['repeated_count'] === 0) {
                return $value;
            }

            $template = $this->templates[$repetitions->id]['templates'][$definitionLevel];

            if (!is_array($template)) {
                throw new InvalidArgumentException('Template for maxDefinitionLevel must be an array');
            }

            return $this->pushValueToLevel($template, $value);
        }

        return $this->templates[$repetitions->id]['templates'][$definitionLevel];
    }

    private function generateForLevel(Repetitions $repetitions, int $level, int $maxDefinitionLevel): array|NullLevel
    {
        if ($level === $maxDefinitionLevel) {
            $node = [];

            for ($i = 0; $i < ($repetitions->repeatedCount() - 1); $i++) {
                $node = [$node];
            }

            return $node;
        }

        $repetitionsBranch = $repetitions->left($level + 1);
        $lastRepetition = $repetitionsBranch->last();

        $partialValue = match ($lastRepetition) {
            Repetition::REQUIRED => throw new InvalidArgumentException('Required field cannot be null'),
            Repetition::OPTIONAL => new NullLevel($level),
            Repetition::REPEATED => [],
        };

        foreach ($repetitionsBranch->toArray() as $repetition) {
            if ($repetition->isRepeated()) {
                $partialValue = [$partialValue];
            }
        }

        if (!$lastRepetition->isRepeated()) {
            return $partialValue;
        }

        if (!is_array($partialValue)) {
            throw new InvalidArgumentException(
                'Internal invariant: repeated last repetition must produce an array template',
            );
        }

        if (!array_key_exists(0, $partialValue)) {
            throw new InvalidArgumentException('Internal invariant: repeated template must contain index 0');
        }

        return $partialValue[0];
    }

    /**
     * When recreating the value for given definition level, in order to not repeate the same dumb operation each
     * time we encounter definition level below maximum definition level we once generate templates for each level.
     *
     * Repetitions->id is just a string representation of all repetitions in the column.
     *
     * By definition column type is irrelevant here, as if two different column types would have the same
     * repetitions, the templates would be the same.
     */
    private function generateTemplates(Repetitions $repetitions, int $maxDefinitionLevel): void
    {
        if (array_key_exists($repetitions->id, $this->templates)) {
            return;
        }

        $this->templates[$repetitions->id] = [
            'templates' => [],
            'max_definition_level' => $maxDefinitionLevel,
            'repeated_count' => $repetitions->repeatedCount(),
            'is_flat' => $repetitions->count() === 1,
            'is_first_required' => $repetitions->first()->isRequired(),
        ];

        for ($d = 0; $d <= $maxDefinitionLevel; $d++) {
            $this->templates[$repetitions->id]['templates'][$d] = $this->generateForLevel(
                $repetitions,
                $d,
                $maxDefinitionLevel,
            );
        }
    }

    private function pushValueToLevel(array $template, mixed $value): array
    {
        if (empty($template)) {
            return [$value];
        }

        if (array_key_exists(0, $template) && is_array($template[0])) {
            $template[0] = $this->pushValueToLevel($template[0], $value);
        }

        return $template;
    }

    private function validate(mixed $value, int $definitionLevel, int $maxDefinitionLevel): void
    {
        if ($value === null && $definitionLevel === $maxDefinitionLevel) {
            throw new InvalidArgumentException(
                'Value cannot be null for level "'
                . $definitionLevel
                . '" and max definition level "'
                . $maxDefinitionLevel
                . '"',
            );
        }

        if ($value !== null && $definitionLevel < $maxDefinitionLevel) {
            throw new InvalidArgumentException(
                'Value cannot be not null for level "'
                . $definitionLevel
                . '" and max definition level "'
                . $maxDefinitionLevel
                . '"',
            );
        }

        if ($definitionLevel > $maxDefinitionLevel) {
            throw new InvalidArgumentException(
                'Given definition level "'
                . $definitionLevel
                . '"  is greater than max level, "'
                . $maxDefinitionLevel
                . '"',
            );
        }
    }
}
