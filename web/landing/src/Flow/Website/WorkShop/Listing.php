<?php

declare(strict_types=1);

namespace Flow\Website\WorkShop;

use Flow\Types\Type;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * @type ListingShape = array{
 *     name: string,
 *     slug: string,
 *     blurb: string,
 *     features: list<string>,
 *     available: bool,
 *     price: null|string,
 *     price_note: null|string,
 *     badge: null|string,
 *     highlight: bool,
 *     route: null|string,
 *     image: null|string
 * }
 */
final readonly class Listing
{
    /**
     * @param list<string> $features
     */
    public function __construct(
        public string $name,
        public string $slug,
        public string $category,
        public string $blurb,
        public array $features,
        public bool $available,
        public ?string $price = null,
        public ?string $priceNote = null,
        public ?string $badge = null,
        public bool $highlight = false,
        public ?string $route = null,
        public ?string $image = null,
    ) {}

    public static function fromArray(array $data, string $category): self
    {
        $listing = self::schema()->assert($data);

        return new self(
            $listing['name'],
            $listing['slug'],
            $category,
            $listing['blurb'],
            $listing['features'],
            $listing['available'],
            $listing['price'],
            $listing['price_note'],
            $listing['badge'],
            $listing['highlight'],
            $listing['route'],
            $listing['image'],
        );
    }

    /**
     * @return Type<ListingShape>
     */
    public static function schema(): Type
    {
        return type_structure([
            'name' => type_string(),
            'slug' => type_string(),
            'blurb' => type_string(),
            'features' => type_list(type_string()),
            'available' => type_boolean(),
            'price' => type_optional(type_string()),
            'price_note' => type_optional(type_string()),
            'badge' => type_optional(type_string()),
            'highlight' => type_boolean(),
            'route' => type_optional(type_string()),
            'image' => type_optional(type_string()),
        ]);
    }
}
