<?php
declare(strict_types=1);

namespace rabbit\db\redis\Commands;

/**
 * Trait ClusterSlot
 * @package rabbit\db\redis\Commands
 */
trait ClusterSlot
{
    /**
     * @return int|null
     */
    public function getSlot()
    {
        return 0;
    }
}