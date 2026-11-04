<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double\Api;

use DateTimeImmutable;

final class Pipeline implements Describable
{
    use Chain;

    public const string NAME = 'pipeline';

    private const string HIDDEN = 'hidden';

    public static function create(): self
    {
        return new self();
    }

    public static function inMode(): Mode
    {
        return Mode::FAST;
    }

    public function __toString(): string
    {
        return 'pipeline';
    }

    public function describe(): string
    {
        return 'pipeline';
    }

    public function step(): Step|DateTimeImmutable
    {
        return new Step();
    }

    public function untyped($value): void {}

    public function configure(Option $option): void {}
}
