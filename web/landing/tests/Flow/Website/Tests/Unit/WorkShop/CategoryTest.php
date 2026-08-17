<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\WorkShop;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Website\WorkShop\Category;
use PHPUnit\Framework\TestCase;

final class CategoryTest extends TestCase
{
    public function test_from_array_builds_listings_carrying_the_category_heading(): void
    {
        $category = Category::fromArray([
            'id' => 'blueprints',
            'heading' => 'Blueprints',
            'subtitle' => 'Working applications you can clone, run and pull apart.',
            'listings' => [
                [
                    'name' => 'Symfony Backoffice Blueprint',
                    'slug' => 'symfony-backoffice',
                    'blurb' => 'A working Symfony back office built on Flow PHP.',
                    'features' => ['Schema drives the database, API and imports'],
                    'available' => true,
                    'price' => '$59',
                    'price_note' => null,
                    'badge' => null,
                    'highlight' => false,
                    'route' => 'work_shop_blueprint_symfony',
                    'image' => 'images/work-shop/symfony-backoffice-app.png',
                ],
            ],
        ]);

        static::assertSame('blueprints', $category->id);
        static::assertSame('Blueprints', $category->heading);
        static::assertSame('Working applications you can clone, run and pull apart.', $category->subtitle);
        static::assertCount(1, $category->listings);
        static::assertSame('symfony-backoffice', $category->listings[0]->slug);
        static::assertSame('Blueprints', $category->listings[0]->category);
    }

    public function test_from_array_rejects_a_category_without_listings_key(): void
    {
        $this->expectException(InvalidTypeException::class);

        Category::fromArray([
            'id' => 'blueprints',
            'heading' => 'Blueprints',
            'subtitle' => 'Working applications you can clone, run and pull apart.',
        ]);
    }

    public function test_from_array_rejects_an_invalid_listing(): void
    {
        $this->expectException(InvalidTypeException::class);

        Category::fromArray([
            'id' => 'blueprints',
            'heading' => 'Blueprints',
            'subtitle' => 'Working applications you can clone, run and pull apart.',
            'listings' => [
                ['name' => 'Broken'],
            ],
        ]);
    }
}
