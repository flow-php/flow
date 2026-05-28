<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPStan\Types\Tests\Unit\data;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function PHPStan\Testing\assertType;

$structure = type_structure(['id' => type_integer(), 'name' => type_string()]);
assertType('Flow\Types\Type<array{id: int, name: string}>&Flow\Types\Type\Logical\StructureType', $structure);

$withOptional = type_structure(['id' => type_integer()], ['nickname' => type_string()]);
assertType('Flow\Types\Type<array{id: int, nickname?: string}>&Flow\Types\Type\Logical\StructureType', $withOptional);
