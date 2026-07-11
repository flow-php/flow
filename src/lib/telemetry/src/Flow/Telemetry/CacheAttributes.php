<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Cache instrumentation attribute keys and span names shared by Flow cache instrumentations.
 *
 * No official cache semantic convention exists yet; these keys follow the direction of the
 * open proposal and will be aligned once it lands.
 *
 * @see https://github.com/open-telemetry/semantic-conventions/issues/1747
 */
final class CacheAttributes
{
    public const string CACHE_KEY = 'cache.key';

    public const string CACHE_KEY_COUNT = 'cache.key_count';

    public const string CACHE_OPERATION = 'cache.operation';

    public const string CACHE_POOL = 'cache.pool';

    public const string CACHE_PREFIX = 'cache.prefix';

    public const string CACHE_TAGS = 'cache.tags';

    public const string CACHE_TAG_COUNT = 'cache.tag_count';

    public const string CACHE_VALUE_TYPE = 'cache.value_type';

    public const string SPAN_CLEAR = 'cache.clear';

    public const string SPAN_DELETE = 'cache.delete';

    public const string SPAN_SET = 'cache.set';
}
