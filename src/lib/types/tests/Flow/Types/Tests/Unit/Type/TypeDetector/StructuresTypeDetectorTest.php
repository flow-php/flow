<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\TypeDetector;

use Flow\Types\Type\TypeDetector;
use PHPUnit\Framework\TestCase;

use function file_get_contents;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function json_decode;

final class StructuresTypeDetectorTest extends TestCase
{
    public function test_detecting_structures_with_nested_arrays(): void
    {
        $typeDetector = new TypeDetector();

        $json = type_string()->assert(file_get_contents(__DIR__ . '/Fixtures/github_user_event.json'));
        // @mago-ignore analysis:mixed-assignment
        $structure = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        $type = $typeDetector->detectType($structure);

        static::assertEquals(
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
                'payload' => type_map(key_type: type_string(), value_type: type_optional(type_string())),
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
