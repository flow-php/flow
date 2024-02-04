<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Coduo\PHPHumanizer\StringHumanizer;
use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

final class HumanizerExtension extends AbstractExtension
{
    public function getFilters()
    {
        return [
            new TwigFilter('humanize', [$this, 'humanize']),
            new TwigFilter('humanize_file_name', [$this, 'humanizeFileName']),
        ];
    }

    public function humanize(string $fileName, bool $capitalize = true, string $separator = '_', array $forbiddenWords = []) : string
    {
        return StringHumanizer::humanize($fileName, $capitalize, $separator, $forbiddenWords);
    }

    public function humanizeFileName(string $fileName, bool $capitalize = true, string $separator = '_', array $forbiddenWords = []) : string
    {
        return StringHumanizer::humanize($fileName, $capitalize, $separator, $forbiddenWords);
    }
}
