<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Integration;

use Flow\Dremel\{DataShredded, Dremel};
use PHPUnit\Framework\TestCase;

final class DremelListsTest extends TestCase
{
    public function test_optional_list_optional_int32() : void
    {
        $repetitionLevels = [0, 1, 1, 0, 0, 0, 1, 1];
        $definitionLevels = [3, 2, 3, 0, 1, 3, 3, 3];
        $values = [1, 3, 4, 5, 6];
        $maxDefinitionLevel = 3;
        $repetitions = ['OPTIONAL', 'REPEATED', 'OPTIONAL'];

        $expectedOutput = [
            [1, null, 3],
            null,
            [],
            [4, 5, 6],
        ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_optional_list_optional_list_optional_list_optional_int32() : void
    {
        $repetitionLevels = [0, 0, 0, 0, 0, 0, 0, 0];
        $definitionLevels = [0, 1, 2, 3, 4, 5, 6, 7];
        $values = [1];
        $repetitions = [
            'OPTIONAL', // 1 - 1
            'REPEATED', // 1 - 2
            'OPTIONAL', // 1 - 3
            'REPEATED', // 1 - 4
            'OPTIONAL', // 1 - 5
            'REPEATED', // 1 - 6
            'OPTIONAL',  // 1 - 7
        ];
        $maxDefinitionLevel = 7;

        $expectedOutput =
            [
                null,
                [],
                [null],
                [[]],
                [[null]],
                [[[]]],
                [[[null]]],
                [[[1]]],
            ];

        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_optional_list_required_list_optional_list_optional_int32() : void
    {
        $repetitionLevels = [0, 0, 0, 0, 0, 0, 0];
        $definitionLevels = [0, 1, 2, 3, 4, 5, 6];
        $values = [1];
        $repetitions = [
            'OPTIONAL', // 1 - d0 === null
            'REPEATED', // 1 - d1 === []
            'REQUIRED', // 0 - d1 === []
            'REPEATED', // 1 - d2 === [[]]
            'OPTIONAL', // 1 - d3 === [[null]]
            'REPEATED', // 1 - d4 === [[[]]]
            'OPTIONAL',  // 1 - d5 === [[[null]]]
            //   - d6 === [[[1]]]
        ];
        $maxDefinitionLevel = 6;

        $expectedOutput =
            [
                null,              // d0
                [],                // d1
                [[]],           // d2
                [[null]],     // d3
                [[[]]],       // d4
                [[[null]]],  // d5
                [[[1]]],      // d6
            ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }

    public function test_require_list_optional_list_optional_list_optional_int32() : void
    {
        $repetitionLevels = [0, 0, 0, 0, 0, 0, 0];
        $definitionLevels = [0, 1, 2, 3, 4, 5, 6];
        $values = [1];
        $repetitions = [
            'REQUIRED', // 0
            'REPEATED', // 1 - 1
            'OPTIONAL', // 1 - 2
            'REPEATED', // 1 - 3
            'OPTIONAL', // 1 - 4
            'REPEATED', // 1 - 5
            'OPTIONAL', // 1 - 6
        ];
        $maxDefinitionLevel = 6;

        $expectedOutput =
            [
                [],
                [null],
                [[]],
                [[null]],
                [[[]]],
                [[[null]]],
                [[[1]]],
            ];

        $assembled = (new Dremel())->assemble(new DataShredded($repetitionLevels, $definitionLevels, $values), $repetitions, $maxDefinitionLevel);
        $shredded = (new Dremel())->shred($expectedOutput, $repetitions);

        self::assertEquals($expectedOutput, $assembled->rows);
        self::assertEquals($repetitionLevels, $shredded->repetitionLevels);
        self::assertEquals($definitionLevels, $shredded->definitionLevels);
        self::assertEquals($values, $shredded->values);
    }
}
