<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\Footer;

final class FooterMother
{
    /**
     * @param array<array-key, mixed> $schema raw normalized schema, e.g. $schema->normalize()
     * @param array<int, \Flow\Floe\Section> $sections
     * @param array<string, array<array-key, mixed>|bool|float|int|string> $metadata
     */
    public static function footer(
        array $schema = [],
        array $sections = [],
        int $totalRows = 0,
        array $metadata = [],
    ): Footer {
        /** @var array<int, array<string, mixed>> $schema */
        return new Footer(1, 'test-writer', $schema, $sections, $totalRows, Metadata::fromArray($metadata));
    }
}
