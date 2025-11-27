<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit;

use Flow\PgQuery\NamedParameterNormalizer;
use PHPUnit\Framework\TestCase;

final class NamedParameterNormalizerTest extends TestCase
{
    public function test_extract_parameters_multiple() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            ['id' => 1, 'name' => 2, 'status' => 3],
            $normalizer->extractParameters('SELECT * FROM users WHERE id = :id AND name = :name AND status = :status')
        );
    }

    public function test_extract_parameters_reuses_position_for_same_parameter() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            ['id' => 1],
            $normalizer->extractParameters('SELECT * FROM users WHERE id = :id OR parent_id = :id')
        );
    }

    public function test_extract_parameters_single() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(['id' => 1], $normalizer->extractParameters('SELECT * FROM users WHERE id = :id'));
    }

    public function test_extract_parameters_without_parameters() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame([], $normalizer->extractParameters('SELECT * FROM users'));
    }

    public function test_normalize_multiple_parameters() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            'SELECT * FROM users WHERE id = $1 AND name = $2 AND status = $3',
            $normalizer->normalize('SELECT * FROM users WHERE id = :id AND name = :name AND status = :status')
        );
    }

    public function test_normalize_parameters_with_underscores() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            'SELECT * FROM users WHERE created_at > $1 AND created_at < $2',
            $normalizer->normalize('SELECT * FROM users WHERE created_at > :start_date AND created_at < :end_date')
        );
    }

    public function test_normalize_preserves_postgresql_type_casts() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            'SELECT *::text FROM users WHERE id = $1',
            $normalizer->normalize('SELECT *::text FROM users WHERE id = :id')
        );
    }

    public function test_normalize_preserves_string_literals_with_colons() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            "SELECT * FROM users WHERE time = '12:30:00'",
            $normalizer->normalize("SELECT * FROM users WHERE time = '12:30:00'")
        );
    }

    public function test_normalize_reuses_position_for_same_parameter() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            'SELECT * FROM users WHERE id = $1 OR parent_id = $1',
            $normalizer->normalize('SELECT * FROM users WHERE id = :id OR parent_id = :id')
        );
    }

    public function test_normalize_single_parameter() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame(
            'SELECT * FROM users WHERE id = $1',
            $normalizer->normalize('SELECT * FROM users WHERE id = :id')
        );
    }

    public function test_normalize_without_parameters() : void
    {
        $normalizer = new NamedParameterNormalizer();

        self::assertSame('SELECT * FROM users', $normalizer->normalize('SELECT * FROM users'));
    }
}
