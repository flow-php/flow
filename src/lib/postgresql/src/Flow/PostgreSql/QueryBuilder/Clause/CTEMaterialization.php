<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\CTEMaterialize;

/**
 * CTE materialization enum.
 */
enum CTEMaterialization : string
{
    case DEFAULT = 'DEFAULT';
    case MATERIALIZED = 'MATERIALIZED';
    case NOT_MATERIALIZED = 'NOT MATERIALIZED';

    public static function fromProtobuf(int $materialize) : self
    {
        return match ($materialize) {
            CTEMaterialize::CTEMaterializeAlways => self::MATERIALIZED,
            CTEMaterialize::CTEMaterializeNever => self::NOT_MATERIALIZED,
            default => self::DEFAULT,
        };
    }

    public function toProtobuf() : int
    {
        return match ($this) {
            self::DEFAULT => CTEMaterialize::CTEMaterializeDefault,
            self::MATERIALIZED => CTEMaterialize::CTEMaterializeAlways,
            self::NOT_MATERIALIZED => CTEMaterialize::CTEMaterializeNever,
        };
    }
}
