<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\StdOut\Filter;

final class Intercept extends \php_user_filter
{
    public static string $buffer = '';

    /**
     * @param resource $in
     * @param resource $out
     * @param int $consumed
     * @param bool $closing
     */
    public function filter($in, $out, &$consumed, bool $closing) : int
    {
        while ($bucket = stream_bucket_make_writeable($in)) {
            self::$buffer .= $bucket->data;
            $consumed += (int) $bucket->datalen;
        }

        return PSFS_FEED_ME;
    }
}
