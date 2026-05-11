<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\CmdType;
use Flow\PostgreSql\QueryBuilder\Schema\Rule\RuleEvent;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerLevel;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerTiming;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class TriggerEnumsTest extends TestCase
{
    #[Test]
    public function test_rule_event_delete_maps_to_cmd_type(): void
    {
        static::assertSame(CmdType::CMD_DELETE, RuleEvent::DELETE->value);
    }

    #[Test]
    public function test_rule_event_insert_maps_to_cmd_type(): void
    {
        static::assertSame(CmdType::CMD_INSERT, RuleEvent::INSERT->value);
    }

    #[Test]
    public function test_rule_event_select_maps_to_cmd_type(): void
    {
        static::assertSame(CmdType::CMD_SELECT, RuleEvent::SELECT->value);
    }

    #[Test]
    public function test_rule_event_update_maps_to_cmd_type(): void
    {
        static::assertSame(CmdType::CMD_UPDATE, RuleEvent::UPDATE->value);
    }

    #[Test]
    public function test_trigger_event_delete_has_value_8(): void
    {
        static::assertSame(8, TriggerEvent::DELETE->value);
    }

    #[Test]
    public function test_trigger_event_insert_has_value_4(): void
    {
        static::assertSame(4, TriggerEvent::INSERT->value);
    }

    #[Test]
    public function test_trigger_event_truncate_has_value_32(): void
    {
        static::assertSame(32, TriggerEvent::TRUNCATE->value);
    }

    #[Test]
    public function test_trigger_event_update_has_value_16(): void
    {
        static::assertSame(16, TriggerEvent::UPDATE->value);
    }

    #[Test]
    public function test_trigger_events_can_be_combined_as_bitmask(): void
    {
        $combined = TriggerEvent::INSERT->value | TriggerEvent::UPDATE->value | TriggerEvent::DELETE->value;

        static::assertSame(28, $combined);
        static::assertTrue(($combined & TriggerEvent::INSERT->value) !== 0);
        static::assertTrue(($combined & TriggerEvent::UPDATE->value) !== 0);
        static::assertTrue(($combined & TriggerEvent::DELETE->value) !== 0);
        static::assertFalse(($combined & TriggerEvent::TRUNCATE->value) !== 0);
    }

    #[Test]
    public function test_trigger_level_row_returns_true(): void
    {
        static::assertTrue(TriggerLevel::ROW->toBool());
    }

    #[Test]
    public function test_trigger_level_statement_returns_false(): void
    {
        static::assertFalse(TriggerLevel::STATEMENT->toBool());
    }

    #[Test]
    public function test_trigger_timing_after_has_value_0(): void
    {
        static::assertSame(0, TriggerTiming::AFTER->value);
    }

    #[Test]
    public function test_trigger_timing_before_has_value_2(): void
    {
        static::assertSame(2, TriggerTiming::BEFORE->value);
    }

    #[Test]
    public function test_trigger_timing_instead_of_has_value_64(): void
    {
        static::assertSame(64, TriggerTiming::INSTEAD_OF->value);
    }
}
