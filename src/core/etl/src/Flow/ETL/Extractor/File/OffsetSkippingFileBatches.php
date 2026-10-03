<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

/**
 * Skips the window's offset itself - a whole file the offset covers yields nothing - so the first row it yields is
 * the first one past the offset.
 */
interface OffsetSkippingFileBatches extends FileBatches {}
