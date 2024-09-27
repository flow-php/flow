<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\TypeDetector;

use function Flow\ETL\DSL\{structure_element, type_boolean, type_int, type_map, type_string, type_structure};
use Flow\ETL\PHP\Type\TypeDetector;
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
                structure_element('id', type_string()),
                structure_element('type', type_string()),
                structure_element('actor', type_structure([
                    structure_element('id', type_int()),
                    structure_element('login', type_string()),
                    structure_element('display_login', type_string()),
                    structure_element('gravatar_id', type_string()),
                    structure_element('url', type_string()),
                    structure_element('avatar_url', type_string()),
                ])),
                structure_element('repo', type_structure([
                    structure_element('id', type_int()),
                    structure_element('name', type_string()),
                    structure_element('url', type_string()),
                ])),
                structure_element('payload', type_map(
                    key_type: type_string(),
                    value_type: type_string(true)
                )),
                structure_element('public', type_boolean()),
                structure_element('created_at', type_string()),
                structure_element('org', type_structure([
                    structure_element('id', type_int()),
                    structure_element('login', type_string()),
                    structure_element('gravatar_id', type_string()),
                    structure_element('url', type_string()),
                    structure_element('avatar_url', type_string()),
                ])),
            ]),
            $type,
        );
    }
}
