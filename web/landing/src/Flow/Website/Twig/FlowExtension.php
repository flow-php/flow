<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Override;
use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

use function is_string;
use function str_pad;
use function stristr;

final class FlowExtension extends AbstractExtension
{
    #[Override]
    public function getFilters(): array
    {
        return [
            new TwigFilter('strpad', $this->strpad(...), ['is_safe' => ['html']]),
        ];
    }

    public function strpad(
        string|int|float $input,
        int $length,
        string $padString = '',
        string|int $padType = 'left',
    ): string {
        if (!is_string($input)) {
            $input = (string) $input;
        }

        if (is_string($padType)) {
            $padType = match (true) {
                stristr($padType, 'left') !== false => STR_PAD_LEFT,
                stristr($padType, 'both') !== false => STR_PAD_BOTH,
                default => STR_PAD_RIGHT,
            };
        }

        return str_pad($input, $length, $padString, $padType);
    }
}
