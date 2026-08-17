<?php

declare(strict_types=1);

namespace Flow\Website\WorkShop;

use Flow\Types\Type;

use function array_map;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * @import-type ListingShape from Listing
 *
 * @type CategoryShape = array{
 *     id: string,
 *     heading: string,
 *     subtitle: string,
 *     listings: list<ListingShape>
 * }
 */
final readonly class Category
{
    /**
     * @param list<Listing> $listings
     */
    public function __construct(
        public string $id,
        public string $heading,
        public string $subtitle,
        public array $listings,
    ) {}

    public static function fromArray(array $data): self
    {
        $category = self::schema()->assert($data);

        return new self($category['id'], $category['heading'], $category['subtitle'], array_map(
            static fn(array $listing): Listing => Listing::fromArray($listing, $category['heading']),
            $category['listings'],
        ));
    }

    /**
     * @return Type<CategoryShape>
     */
    public static function schema(): Type
    {
        return type_structure([
            'id' => type_string(),
            'heading' => type_string(),
            'subtitle' => type_string(),
            'listings' => type_list(Listing::schema()),
        ]);
    }
}
