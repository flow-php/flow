<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Twig\Extension\AbstractExtension;
use Twig\{TwigFilter};

final class FlowExtension extends AbstractExtension
{
    public function getFilters() : array
    {
        return [
            new TwigFilter('strpad', [$this, 'strpad'], ['is_safe' => ['html']]),
        ];
    }

    public function strpad(string|int|float $input, int $length, string $padString = '', string|int $padType = 'left')
    {
        if (!\is_string($input)) {
            $input = (string) $input;
        }

        if (is_string($padType)) {
            switch (true) {
                case stristr($padType, 'left'):
                    $padType = STR_PAD_LEFT;

                    break;
                case stristr($padType, 'both'):
                    $padType = STR_PAD_BOTH;

                    break;

                default:
                    $padType = STR_PAD_RIGHT;
            }

        }

        return str_pad($input, $length, $padString, $padType);
    }
}
