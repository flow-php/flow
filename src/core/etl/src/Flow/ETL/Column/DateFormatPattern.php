<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use function ctype_alpha;
use function strlen;

final readonly class DateFormatPattern
{
    public function __construct(
        public string $format,
    ) {}

    /**
     * @return list<string> the unescaped ASCII letters, in order
     */
    public function letters(): array
    {
        $letters = [];

        for ($i = 0, $length = strlen($this->format); $i < $length; $i++) {
            if ($this->format[$i] === '\\') {
                $i++;

                continue;
            }

            if (ctype_alpha($this->format[$i])) {
                $letters[] = $this->format[$i];
            }
        }

        return $letters;
    }

    /**
     * @return list<array{string, ''|'u'|'v'}> the format cut at every unescaped u / v: [segment, the letter that ends it]
     */
    public function segments(): array
    {
        $segments = [];
        $segment = '';

        for ($i = 0, $length = strlen($this->format); $i < $length; $i++) {
            $character = $this->format[$i];

            if ($character === '\\') {
                $segment .= $character . ($this->format[$i + 1] ?? '');
                $i++;

                continue;
            }

            if ($character === 'u' || $character === 'v') {
                $segments[] = [$segment, $character];
                $segment = '';

                continue;
            }

            $segment .= $character;
        }

        if ($segment !== '') {
            $segments[] = [$segment, ''];
        }

        return $segments;
    }
}
