<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\Footer;

final class FooterMother
{
    /**
     * @param array<array-key, mixed> $schemas raw normalized schema lists, e.g. [$schema->normalize()]
     * @param array<array-key, mixed> $fileSchema raw normalized schema, e.g. $schema->normalize()
     * @param array<int, \Flow\Floe\Section> $sections
     * @param array<string, string> $partitions
     * @param array<string, array<array-key, mixed>|bool|float|int|string> $metadata
     */
    public static function footer(
        array $schemas = [],
        array $fileSchema = [],
        array $sections = [],
        array $partitions = [],
        int $totalRows = 0,
        array $metadata = [],
    ): Footer {
        /**
         * @var array<int, array<int, array<string, mixed>>> $schemas
         * @var array<int, array<string, mixed>> $fileSchema
         */
        return new Footer(
            1,
            'test-writer',
            $schemas,
            $fileSchema,
            $sections,
            $partitions,
            $totalRows,
            Metadata::fromArray($metadata),
        );
    }
}
