<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Context;

final readonly class SchemaInferenceFixtureContext
{
    private const FIXTURES = __DIR__ . '/../Integration/Fixtures/inference';

    /**
     * A column the first row types as integer and the second widens to float.
     */
    public static function wideningPath(): string
    {
        return self::FIXTURES . '/widening.csv';
    }

    /**
     * A column the first row types as integer and the second cannot satisfy at all.
     */
    public static function outgrownPath(): string
    {
        return self::FIXTURES . '/outgrown.csv';
    }

    /**
     * Two sources whose column sets differ: a,b and a,c.
     */
    public static function unionGlob(): string
    {
        return self::FIXTURES . '/union/*.csv';
    }

    /**
     * What wideningPath() reads back once a one-row sample has frozen the schema at ?integer:
     * 1.5 truncates, because integer::cast(1.5) is 1.
     *
     * @return array<int, array{a: int}>
     */
    public static function wideningTruncatedToOneRowSample(): array
    {
        return [['a' => 1], ['a' => 1]];
    }
}
