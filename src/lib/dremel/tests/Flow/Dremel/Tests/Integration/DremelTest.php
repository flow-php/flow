<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Integration;

use Flow\Dremel\{DataShredded, Dremel, Repetition};
use PHPUnit\Framework\TestCase;

final class DremelTest extends TestCase
{
    public function test_flat_01() : void
    {
        $repetitions = [Repetition::OPTIONAL, Repetition::REPEATED, Repetition::REQUIRED];

        self::assertNull(
            (new Dremel())->flatten($repetitions, 0, null)
        );
    }

    public function test_optional_int32() : void
    {
        $repetitionLevels = [0, 0, 0, 0, 0];
        $definitionLevels = [1, 1, 0, 1, 0];
        $values = [1, 2, 3];
        $maxDefinitionLevel = 1;
        $repetitions = [Repetition::OPTIONAL];

        $expectedOutput = [1, 2, null, 3, null];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_int32() : void
    {
        $repetitionLevels = [0, 0, 0];
        $definitionLevels = [0, 0, 0];
        $values = [1, 2, 3];
        $maxDefinitionLevel = 0;
        $repetitions = [
            Repetition::REQUIRED,
        ];

        $expectedOutput = [
            1,
            2,
            3,
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }
}
