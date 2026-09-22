<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\Footer;
use Flow\Floe\Statistics;

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
        int $rows = 0,
        int $byteSize = 0,
        array $metadata = [],
    ): Footer {
        /** @var array<int, array<string, mixed>> $schema */
        return new Footer(
            1,
            'test-writer',
            $schema,
            $sections,
            new Statistics($rows, $byteSize),
            Metadata::fromArray($metadata),
        );
    }
}
