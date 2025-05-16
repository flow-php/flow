<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\TypeDetector;

use function Flow\Types\DSL\{type_boolean, type_map, type_string, type_structure};
use function Flow\Types\DSL\{type_integer, type_optional};
use Flow\Types\Type\TypeDetector;
use PHPUnit\Framework\TestCase;

final class StructuresTypeDetectorTest extends TestCase
{
    public function test_detecting_structures_with_nested_arrays() : void
    {
        $typeDetector = new TypeDetector();

        $structure = \json_decode(\file_get_contents(__DIR__ . '/Fixtures/github_user_event.json'), true, 512, JSON_THROW_ON_ERROR);
        $type = $typeDetector->detectType($structure);

        self::assertEquals(
            type_structure([
                'id' => type_string(),
                'type' => type_string(),
                'actor' => type_structure([
                    'id' => type_integer(),
                    'login' => type_string(),
                    'display_login' => type_string(),
                    'gravatar_id' => type_string(),
                    'url' => type_string(),
                    'avatar_url' => type_string(),
                ]),
                'repo' => type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
                    'url' => type_string(),
                ]),
                'payload' => type_map(
                    key_type: type_string(),
                    value_type: type_optional(type_string())
                ),
                'public' => type_boolean(),
                'created_at' => type_string(),
                'org' => type_structure([
                    'id' => type_integer(),
                    'login' => type_string(),
                    'gravatar_id' => type_string(),
                    'url' => type_string(),
                    'avatar_url' => type_string(),
                ]),
            ]),
            $type,
        );
    }
}
