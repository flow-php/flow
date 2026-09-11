<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPStan\Types\Tests\Unit\data;

use Flow\Types\Type\Logical\StructureType;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function PHPStan\Testing\assertType;

$structure = type_structure(['id' => type_integer(), 'name' => type_string()]);
assertType('Flow\Types\Type<array{id: int, name: string}>&Flow\Types\Type\Logical\StructureType', $structure);

$withOptional = type_structure([
    'id' => type_integer(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
]);
assertType('Flow\Types\Type<array{id: int, nickname?: string}>&Flow\Types\Type\Logical\StructureType', $withOptional);

$interleaved = type_structure([
    'id' => type_integer(),
    'nick' => structure_element('nick', type_string(), optional: true),
    'name' => type_string(),
]);
assertType(
    'Flow\Types\Type<array{id: int, nick?: string, name: string}>&Flow\Types\Type\Logical\StructureType',
    $interleaved,
);

$requiredMarker = type_structure(['id' => type_integer(), 'nick' => structure_element('nick', type_string())]);
assertType('Flow\Types\Type<array{id: int, nick: string}>&Flow\Types\Type\Logical\StructureType', $requiredMarker);

$fromElements = StructureType::fromElements([
    'id' => type_integer(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
]);
assertType('Flow\Types\Type\Logical\StructureType<array<int|string>>', $fromElements);
