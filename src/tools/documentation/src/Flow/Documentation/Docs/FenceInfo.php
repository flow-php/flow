<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use function array_slice;
use function count;
use function explode;
use function implode;
use function in_array;
use function preg_split;
use function trim;

final readonly class FenceInfo
{
    public function __construct(
        public string $language,
        public ?string $modifier,
    ) {}

    /**
     * CommonMark takes the first word of the info string as the language class, so a trailing
     * space or a modifier after the language still renders as language-<lang>.
     */
    public static function parse(string $info): self
    {
        $words = preg_split('/\s+/', trim($info), -1, PREG_SPLIT_NO_EMPTY) ?: [];

        if ($words === []) {
            return new self('', null);
        }

        return new self($words[0], count($words) === 1 ? null : implode(' ', array_slice($words, 1)));
    }

    public function isPhp(): bool
    {
        return $this->language === 'php';
    }

    public function isIgnored(): bool
    {
        return $this->modifier !== null && in_array('ignore', explode(' ', $this->modifier), true);
    }
}
