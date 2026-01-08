<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\Cost;
use PHPUnit\Framework\TestCase;

final class CostTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse() : void
    {
        $original = new Cost(
            startupCost: 10.5,
            totalCost: 150.75,
        );

        $normalized = $original->normalize();
        $restored = Cost::fromArray($normalized);

        self::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance() : void
    {
        $data = [
            'startup_cost' => 10.5,
            'total_cost' => 150.75,
        ];

        $cost = Cost::fromArray($data);

        self::assertSame(10.5, $cost->startupCost());
        self::assertSame(150.75, $cost->totalCost());
    }

    public function test_normalize_returns_all_fields() : void
    {
        $cost = new Cost(
            startupCost: 10.5,
            totalCost: 150.75,
        );

        $normalized = $cost->normalize();

        self::assertSame(10.5, $normalized['startup_cost']);
        self::assertSame(150.75, $normalized['total_cost']);
    }

    public function test_normalize_returns_expected_keys() : void
    {
        $cost = new Cost(
            startupCost: 0.0,
            totalCost: 100.0,
        );

        $normalized = $cost->normalize();

        $expectedKeys = [
            'startup_cost',
            'total_cost',
        ];

        self::assertSame($expectedKeys, \array_keys($normalized));
    }
}
