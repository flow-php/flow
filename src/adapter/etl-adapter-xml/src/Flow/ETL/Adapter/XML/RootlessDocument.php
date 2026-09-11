<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\Filesystem\SourceStream;

use function ltrim;
use function str_starts_with;
use function strlen;
use function strpos;
use function substr;

final readonly class RootlessDocument
{
    private const string BOM = "\xEF\xBB\xBF";

    private const string WHITESPACE = " \t\r\n";

    /**
     * An optional UTF-8 BOM, then only whitespace, comments and processing instructions - what libxml 2.12 and later
     * report as an empty document. Reading stops at the first byte that proves the document has more.
     *
     * @param int<1, max> $bufferSize bytes read from the stream at a time
     */
    public function matches(SourceStream $stream, int $bufferSize): bool
    {
        $head = '';
        $atStart = true;
        $closer = null;

        foreach ($stream->iterate($bufferSize) as $chunk) {
            $head .= $chunk;

            if ($atStart) {
                if (strlen($head) < strlen(self::BOM) && str_starts_with(self::BOM, $head)) {
                    continue;
                }

                if (str_starts_with($head, self::BOM)) {
                    $head = substr($head, strlen(self::BOM));
                }

                $atStart = false;
            }

            while (true) {
                if ($closer !== null) {
                    $end = strpos($head, $closer);

                    if ($end === false) {
                        // the closer may be split across chunks, so its first bytes are kept
                        $head = substr($head, -(strlen($closer) - 1));

                        continue 2;
                    }

                    $head = substr($head, $end + strlen($closer));
                    $closer = null;
                }

                $head = ltrim($head, self::WHITESPACE);

                if (str_starts_with($head, '<?')) {
                    $head = substr($head, 2);
                    $closer = '?>';
                } elseif (str_starts_with($head, '<!--')) {
                    $head = substr($head, 4);
                    $closer = '-->';
                } elseif (str_starts_with('<!--', $head)) {
                    continue 2;
                } else {
                    return false;
                }
            }
        }

        return $closer === null && $head === '';
    }
}
