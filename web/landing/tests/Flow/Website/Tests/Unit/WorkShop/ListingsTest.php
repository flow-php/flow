<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\WorkShop;

use Flow\Website\WorkShop\Listings;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_unique;
use function array_values;

final class ListingsTest extends TestCase
{
    public function test_all_returns_the_categories_in_display_order(): void
    {
        static::assertSame(
            ['blueprints', 'ai', 'sponsoring', 'consulting'],
            array_map(static fn($category): string => $category->id, (new Listings())->all()),
        );
    }

    public function test_every_listing_has_a_unique_slug(): void
    {
        $slugs = [];

        foreach ((new Listings())->all() as $category) {
            foreach ($category->listings as $listing) {
                $slugs[] = $listing->slug;
            }
        }

        static::assertSame($slugs, array_values(array_unique($slugs)));
    }

    public function test_every_available_listing_has_a_route(): void
    {
        foreach ((new Listings())->all() as $category) {
            foreach ($category->listings as $listing) {
                if ($listing->available) {
                    static::assertNotNull($listing->route, $listing->slug);
                    static::assertNotNull($listing->image, $listing->slug);
                }
            }
        }
    }

    public function test_find_by_slug_returns_the_listing_with_its_category(): void
    {
        $listing = (new Listings())->findBySlug('claude-skills');

        static::assertSame('Claude Code Skills', $listing->name);
        static::assertSame('AI', $listing->category);
        static::assertSame('$10', $listing->price);
    }

    public function test_find_by_slug_throws_for_an_unknown_slug(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Listing not found: unknown');

        (new Listings())->findBySlug('unknown');
    }
}
