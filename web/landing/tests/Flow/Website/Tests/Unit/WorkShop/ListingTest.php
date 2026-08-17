<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\WorkShop;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Website\WorkShop\Listing;
use PHPUnit\Framework\TestCase;

final class ListingTest extends TestCase
{
    public function test_from_array_maps_all_fields(): void
    {
        $listing = Listing::fromArray([
            'name' => 'Sponsoring — 6 Months',
            'slug' => 'sponsoring-6-months',
            'blurb' => 'The same sponsorship in one payment every six months.',
            'features' => ['Everything from the monthly sponsorship', 'Save 3% vs paying monthly'],
            'available' => true,
            'price' => '$29',
            'price_note' => 'save 3%',
            'badge' => 'Best value',
            'highlight' => true,
            'route' => 'work_shop_sponsoring_6m',
            'image' => 'images/work-shop/sponsoring-6m.svg',
        ], 'Sponsoring');

        static::assertSame('Sponsoring — 6 Months', $listing->name);
        static::assertSame('sponsoring-6-months', $listing->slug);
        static::assertSame('Sponsoring', $listing->category);
        static::assertSame('The same sponsorship in one payment every six months.', $listing->blurb);
        static::assertSame(
            ['Everything from the monthly sponsorship', 'Save 3% vs paying monthly'],
            $listing->features,
        );
        static::assertTrue($listing->available);
        static::assertSame('$29', $listing->price);
        static::assertSame('save 3%', $listing->priceNote);
        static::assertSame('Best value', $listing->badge);
        static::assertTrue($listing->highlight);
        static::assertSame('work_shop_sponsoring_6m', $listing->route);
        static::assertSame('images/work-shop/sponsoring-6m.svg', $listing->image);
    }

    public function test_from_array_accepts_null_for_optional_fields(): void
    {
        $listing = Listing::fromArray([
            'name' => 'More Blueprints',
            'slug' => 'more-blueprints',
            'blurb' => 'More are being built.',
            'features' => ['Fully tested and statically analysed'],
            'available' => false,
            'price' => null,
            'price_note' => null,
            'badge' => null,
            'highlight' => false,
            'route' => null,
            'image' => null,
        ], 'Blueprints');

        static::assertNull($listing->price);
        static::assertNull($listing->priceNote);
        static::assertNull($listing->badge);
        static::assertNull($listing->route);
        static::assertNull($listing->image);
    }

    public function test_from_array_rejects_missing_keys(): void
    {
        $this->expectException(InvalidTypeException::class);

        Listing::fromArray([
            'name' => 'Consulting',
            'slug' => 'consulting',
        ], 'Consulting');
    }

    public function test_from_array_rejects_invalid_types(): void
    {
        $this->expectException(InvalidTypeException::class);

        Listing::fromArray([
            'name' => 'Consulting',
            'slug' => 'consulting',
            'blurb' => 'Blurb',
            'features' => 'not-a-list',
            'available' => true,
            'price' => null,
            'price_note' => null,
            'badge' => null,
            'highlight' => false,
            'route' => null,
            'image' => null,
        ], 'Consulting');
    }
}
