<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Integration;

use Flow\Dremel\{DataShredded, Dremel, Repetition};
use PHPUnit\Framework\TestCase;

final class DremelStructsTest extends TestCase
{
    public function test_optional_struct__optional_string__() : void
    {

        $repetitionLevels = [0, 0, 0];
        $definitionLevels = [0, 1, 2];
        $values = ['Alice'];
        $maxDefinitionLevel = 2;
        $repetitions = [
            Repetition::OPTIONAL, // 0 === NullableRow
            Repetition::OPTIONAL, // 1 === null
            // 2 === "Alice"
        ];

        $expectedOutput = [
            null,
            null,
            'Alice',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_optional_struct__optional_struct__optional_string____() : void
    {
        $repetitionLevels = [0, 0, 0, 0];
        $definitionLevels = [0, 1, 2, 3];
        $values = ['Bob'];
        $maxDefinitionLevel = 3;
        $repetitions = [
            Repetition::OPTIONAL,
            Repetition::OPTIONAL,
            Repetition::OPTIONAL,
        ];

        $expectedOutput = [
            null,
            null,
            null,
            'Bob',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_struct__optional_string__() : void
    {

        $repetitionLevels = [0, 0, 0, 0];
        $definitionLevels = [1, 1, 0, 1];
        $values = ['Alice', 'Bob', 'John'];
        $maxDefinitionLevel = 1;
        $repetitions = [
            Repetition::REQUIRED,
            Repetition::OPTIONAL, // 0 === null / 1 === "Alice"
        ];

        $expectedOutput = [
            'Alice',
            'Bob',
            null,
            'John',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_struct__optional_struct__optional_string____() : void
    {
        $repetitionLevels = [0, 0, 0];
        $definitionLevels = [0, 1, 2];
        $values = ['Bob'];
        $maxDefinitionLevel = 2;
        $repetitions = [
            Repetition::REQUIRED,
            Repetition::OPTIONAL,
            Repetition::OPTIONAL,
        ];

        $expectedOutput = [
            [],
            [[]],
            [['Bob']],
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        // self::assertEquals($definitionLevels, $shredded->definitions);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_struct__required_struct__optional_string____() : void
    {
        $repetitionLevels = [0, 0];
        $definitionLevels = [0, 1];
        $values = ['Bob'];
        $maxDefinitionLevel = 1;
        $repetitions = [
            Repetition::REQUIRED,
            Repetition::REQUIRED,
            Repetition::OPTIONAL,
        ];

        $expectedOutput = [
            null,
            'Bob',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        // self::assertEquals($definitionLevels, $shredded->definitions);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_struct__required_struct__required_string____() : void
    {
        $repetitionLevels = [0, 0, 0];
        $definitionLevels = [0, 0, 0];
        $values = ['Alice', 'Bob', 'John'];
        $maxDefinitionLevel = 0;
        $repetitions = [
            Repetition::REQUIRED,
            Repetition::REQUIRED,
            Repetition::REQUIRED,
        ];

        $expectedOutput = [
            'Alice',
            'Bob',
            'John',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_structure__required_string__() : void
    {
        $repetitionLevels = [0];
        $definitionLevels = [0];
        $values = ['Alice'];
        $maxDefinitionLevel = 0;
        $repetitions = [
            Repetition::REQUIRED,
            Repetition::REQUIRED,
        ];

        $expectedOutput = [
            'Alice',
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }
}
