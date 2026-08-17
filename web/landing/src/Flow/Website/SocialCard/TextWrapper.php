<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard;

use RuntimeException;

use function array_slice;
use function count;
use function explode;
use function imagettfbbox;
use function mb_substr;
use function trim;

final readonly class TextWrapper
{
    public function __construct(
        private string $fontPath,
    ) {}

    /**
     * @return non-empty-list<string>
     */
    public function wrap(string $text, float $fontSize, int $maxWidth, ?int $maxLines = null): array
    {
        $lines = [];
        $line = '';

        foreach (explode(' ', trim($text)) as $word) {
            $candidate = $line === '' ? $word : $line . ' ' . $word;

            if ($line === '' || $this->width($candidate, $fontSize) <= $maxWidth) {
                $line = $candidate;

                continue;
            }

            $lines[] = $line;
            $line = $word;
        }

        $lines[] = $line;

        if ($maxLines === null || count($lines) <= $maxLines) {
            return $lines;
        }

        $kept = array_slice($lines, offset: 0, length: $maxLines - 1);
        $last = $lines[$maxLines - 1];

        while ($last !== '' && $this->width($last . '…', $fontSize) > $maxWidth) {
            $last = trim(mb_substr($last, start: 0, length: -1));
        }

        $kept[] = $last . '…';

        return $kept;
    }

    public function width(string $text, float $fontSize): int
    {
        $box = imagettfbbox($fontSize, angle: 0, font_filename: $this->fontPath, string: $text);

        if ($box === false) {
            throw new RuntimeException('Failed to measure text: ' . $text);
        }

        return (int) $box[2] - (int) $box[0];
    }
}
