<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination;

enum SensitiveHeaders: string
{
    case Authorization = 'Authorization';
    case ProxyAuthorization = 'Proxy-Authorization';
    case Cookie = 'Cookie';

    /**
     * @return array<string>
     */
    public static function toArray(): array
    {
        return array_column(self::cases(), 'value');
    }
}
