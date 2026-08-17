<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard;

use Flow\Website\SocialCard\TextWrapper;
use PHPUnit\Framework\TestCase;

use function count;
use function dirname;
use function str_ends_with;

final class TextWrapperTest extends TestCase
{
    public function test_ellipsizes_last_line_when_exceeding_max_lines(): void
    {
        $wrapper = new TextWrapper(dirname(__DIR__, levels: 6) . '/resources/fonts/Cabin-Regular.ttf');

        $lines = $wrapper->wrap(
            'one two three four five six seven eight nine ten eleven twelve',
            fontSize: 20,
            maxWidth: 200,
            maxLines: 2,
        );

        static::assertCount(2, $lines);
        static::assertTrue(str_ends_with($lines[1], '…'));
        static::assertLessThanOrEqual(200, $wrapper->width($lines[1], fontSize: 20));
    }

    public function test_keeps_short_text_in_a_single_line(): void
    {
        $wrapper = new TextWrapper(dirname(__DIR__, levels: 6) . '/resources/fonts/Cabin-Regular.ttf');

        static::assertSame(['Flow PHP'], $wrapper->wrap('Flow PHP', fontSize: 20, maxWidth: 1000));
    }

    public function test_keeps_word_longer_than_max_width_on_its_own_line(): void
    {
        $wrapper = new TextWrapper(dirname(__DIR__, levels: 6) . '/resources/fonts/Cabin-Regular.ttf');

        static::assertSame(
            ['incomprehensibilities', 'yes'],
            $wrapper->wrap('incomprehensibilities yes', fontSize: 20, maxWidth: 50),
        );
    }

    public function test_width_grows_with_longer_text(): void
    {
        $wrapper = new TextWrapper(dirname(__DIR__, levels: 6) . '/resources/fonts/Cabin-Regular.ttf');

        static::assertGreaterThan(
            $wrapper->width('Flow', fontSize: 20),
            $wrapper->width('Flow PHP Framework', fontSize: 20),
        );
    }

    public function test_wraps_text_into_multiple_lines_fitting_max_width(): void
    {
        $wrapper = new TextWrapper(dirname(__DIR__, levels: 6) . '/resources/fonts/Cabin-Regular.ttf');

        $lines = $wrapper->wrap('Consuming APIs Without SDKs in pure PHP with Flow', fontSize: 20, maxWidth: 200);

        static::assertGreaterThan(1, count($lines));

        foreach ($lines as $line) {
            static::assertLessThanOrEqual(200, $wrapper->width($line, fontSize: 20));
        }
    }
}
