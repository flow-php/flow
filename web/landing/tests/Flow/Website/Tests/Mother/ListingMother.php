<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Mother;

use Flow\Website\WorkShop\Listing;

final class ListingMother
{
    public static function placeholder(): Listing
    {
        return Listing::fromArray([
            'name' => 'More Blueprints',
            'slug' => 'more-blueprints',
            'blurb' => 'More are being built, same standards, different frameworks and different problems.',
            'features' => ['Fully tested and statically analysed'],
            'available' => false,
            'price' => null,
            'price_note' => null,
            'badge' => null,
            'highlight' => false,
            'route' => null,
            'image' => null,
        ], 'Blueprints');
    }

    public static function priced(): Listing
    {
        return Listing::fromArray([
            'name' => 'Symfony Backoffice Blueprint',
            'slug' => 'symfony-backoffice',
            'blurb' => 'A working Symfony back office built on Flow PHP. One schema per model drives the database, the API and the imports.',
            'features' => ['Schema drives the database, API and imports'],
            'available' => true,
            'price' => '$59',
            'price_note' => null,
            'badge' => null,
            'highlight' => false,
            'route' => 'work_shop_blueprint_symfony',
            'image' => 'images/work-shop/symfony-backoffice-app.png',
        ], 'Blueprints');
    }

    public static function unpriced(): Listing
    {
        return Listing::fromArray([
            'name' => 'Consulting',
            'slug' => 'consulting',
            'blurb' => 'Sixteen years of data engineering, software architecture and observability work.',
            'features' => ['Data engineering, architecture, observability'],
            'available' => true,
            'price' => null,
            'price_note' => null,
            'badge' => null,
            'highlight' => false,
            'route' => 'work_shop_consulting',
            'image' => 'images/work-shop/consulting.jpg',
        ], 'Consulting');
    }
}
