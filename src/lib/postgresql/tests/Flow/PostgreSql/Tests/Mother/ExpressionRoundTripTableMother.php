<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_timestamptz;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_column;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_trigger;

final class ExpressionRoundTripTableMother
{
    public static function declared(
        string $schema,
        string $functionName = 'trg_noop_fn',
        string $generationExpression = 'lower(i::text)',
    ): Table {
        return schema_table(
            't',
            [
                schema_column('i', column_type_integer()),
                schema_column('g', column_type_text(), isGenerated: true, generationExpression: $generationExpression),
                schema_column('d', column_type_text(), default: func('lower', [cast(literal(42), column_type_text())])),
                schema_column('email', column_type_text()),
                schema_column('deleted_at', column_type_timestamptz()),
            ],
            indexes: [schema_index('t_email_live', ['email'], unique: true, predicate: 'deleted_at IS NULL')],
            checkConstraints: [schema_check("lower(i::text) <> ''", 't_i_check')],
            triggers: [schema_trigger(
                't_trg',
                't',
                TriggerTiming::BEFORE,
                [TriggerEvent::UPDATE],
                $functionName,
                true,
                'NEW.i > 0',
            )],
            schema: $schema,
        );
    }
}
