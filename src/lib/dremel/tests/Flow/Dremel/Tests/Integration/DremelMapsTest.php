<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Integration;

use Flow\Dremel\{DataShredded, Dremel};
use PHPUnit\Framework\TestCase;

final class DremelMapsTest extends TestCase
{
    public function test_required_map__string_required_int__key() : void
    {
        $repetitionLevels = [0, 1, 1];
        $definitionLevels = [1, 1, 1];
        $values = ['a', 'b', 'c'];
        $maxDefinitionLevel = 1;
        $repetitions = [
            'REQUIRED',
            'REPEATED',
            'REQUIRED',
        ];

        $expectedOutput = [
            ['a', 'b', 'c'],
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_required_map__string_required_int__value() : void
    {
        $repetitionLevels = [0, 1, 1];
        $definitionLevels = [1, 1, 1];
        $values = [1, 2, 3];
        $maxDefinitionLevel = 1;
        $repetitions = [
            'REQUIRED',
            'REPEATED',
            'REQUIRED',
        ];

        $expectedOutput = [
            [1, 2, 3],
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }
}
