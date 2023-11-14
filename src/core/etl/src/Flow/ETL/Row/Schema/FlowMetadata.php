<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

final class FlowMetadata
{
    public const METADATA_ENUM_CASES = 'flow_enum_values';

    public const METADATA_ENUM_CLASS = 'flow_enum_class';

    public const METADATA_LIST_ENTRY_TYPE = 'flow_list_entry_type';

    public const METADATA_MAP_ENTRY_TYPE = 'flow_map_entry_type';

    public const METADATA_OBJECT_ENTRY_TYPE = 'flow_object_entry_type';

    public const METADATA_STRUCTURE_ENTRY_TYPE = 'flow_structure_entry_type';
}
