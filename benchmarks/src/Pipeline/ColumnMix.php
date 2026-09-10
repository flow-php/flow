<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

/**
 * Separates the per-value cast paths: temporal values are parsed, nested values are decoded
 * into containers, scalars take neither path.
 */
enum ColumnMix: string
{
    case nested = 'nested';
    case scalar = 'scalar';
    case temporal = 'temporal';

    /**
     * @return list<string>
     */
    public function columns(): array
    {
        return match ($this) {
            self::nested => ['address', 'notes', 'items'],
            self::scalar => ['order_id', 'seller_id', 'discount', 'email', 'customer'],
            self::temporal => ['created_at', 'updated_at', 'cancelled_at'],
        };
    }
}
