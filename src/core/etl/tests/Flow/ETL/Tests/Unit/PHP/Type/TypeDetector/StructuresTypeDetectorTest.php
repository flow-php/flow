<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\TypeDetector;

use function Flow\ETL\DSL\{type_boolean, type_int, type_map, type_string, type_structure};
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\Tests\FlowTestCase;

final class StructuresTypeDetectorTest extends FlowTestCase
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
                    'id' => type_int(),
                    'login' => type_string(),
                    'display_login' => type_string(),
                    'gravatar_id' => type_string(),
                    'url' => type_string(),
                    'avatar_url' => type_string(),
                ]),
                'repo' => type_structure([
                    'id' => type_int(),
                    'name' => type_string(),
                    'url' => type_string(),
                ]),
                'payload' => type_map(
                    key_type: type_string(),
                    value_type: type_string(true)
                ),
                'public' => type_boolean(),
                'created_at' => type_string(),
                'org' => type_structure([
                    'id' => type_int(),
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
