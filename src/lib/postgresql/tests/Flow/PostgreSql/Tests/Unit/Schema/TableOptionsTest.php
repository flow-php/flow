<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_exclude;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_table_options;
use function Flow\PostgreSql\DSL\schema_trigger;

final class TableOptionsTest extends TestCase
{
    public function test_all_options_set(): void
    {
        $options = schema_table_options(
            foreignKeys: [schema_foreign_key(['user_id'], 'users', ['id'])],
            checkConstraints: [schema_check('price > 0')],
            excludeConstraints: [schema_exclude('USING gist (tsrange WITH &&)')],
            triggers: [schema_trigger('trg_audit', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit')],
            unlogged: true,
            partitionStrategy: PartitionStrategy::RANGE,
            partitionColumns: ['created_at'],
            inherits: ['parent'],
            tablespace: 'fast_storage',
        );

        static::assertCount(1, $options->foreignKeys);
        static::assertSame('users', $options->foreignKeys[0]->referenceTable);
        static::assertCount(1, $options->checkConstraints);
        static::assertCount(1, $options->excludeConstraints);
        static::assertCount(1, $options->triggers);
        static::assertSame('trg_audit', $options->triggers[0]->name);
        static::assertTrue($options->unlogged);
        static::assertSame(PartitionStrategy::RANGE, $options->partitionStrategy);
        static::assertSame(['created_at'], $options->partitionColumns);
        static::assertSame(['parent'], $options->inherits);
        static::assertSame('fast_storage', $options->tablespace);
    }

    public function test_defaults(): void
    {
        $options = schema_table_options();

        static::assertSame([], $options->foreignKeys);
        static::assertSame([], $options->checkConstraints);
        static::assertSame([], $options->excludeConstraints);
        static::assertSame([], $options->triggers);
        static::assertFalse($options->unlogged);
        static::assertNull($options->partitionStrategy);
        static::assertSame([], $options->partitionColumns);
        static::assertSame([], $options->inherits);
        static::assertNull($options->tablespace);
    }
}
