<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

interface DeclareCursorOptionsStep extends DeclareCursorFinalStep
{
    /**
     * Make the cursor read binary data.
     */
    public function binary() : self;

    /**
     * Make the cursor non-scrollable (forward-only).
     * This is the default and recommended for ETL operations.
     */
    public function noScroll() : self;

    /**
     * Make the cursor scrollable (allows FETCH BACKWARD, FETCH ABSOLUTE, etc.).
     */
    public function scroll() : self;

    /**
     * Allow the cursor to remain open after the transaction commits.
     */
    public function withHold() : self;
}
